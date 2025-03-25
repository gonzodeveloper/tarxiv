# Listen for new TNS Alerts
from .utils import read_config, get_logger
from .database import TarxivDB
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build

from collections import OrderedDict
from bs4 import BeautifulSoup
import pandas as pd
import requests
import zipfile
import base64
import json
import time
import io
import os


class TarxivTNS:
    def __init__(self, config_dir, log_name=None):
        # General parameters
        self.config = read_config(config_dir + "config.yml")
        log_file = (
            os.path.join(self.config["log_dir"], log_name)
            if log_name is not None
            else None
        )
        self.logger = get_logger("tarxiv_tns", self.config["log_level"], log_file)

        # Set attributes
        self.site = self.config["tns"]["site"]
        self.api_key = self.config["tns"]["api_key"]

        # Create marker
        tns_marker_dict = {
            "tns_id": self.config["tns"]["id"],
            "type": self.config["tns"]["type"],
            "name": self.config["tns"]["name"],
        }
        self.marker = "tns_marker" + json.dumps(tns_marker_dict, separators=(",", ":"))

        # Logging
        self.logger.info("connecting to gmail api")
        # Get gmail token
        self.creds = None
        # Absolute paths
        token = os.path.join(config_dir, self.config["gmail"]["token_name"])
        secrets = os.path.join(config_dir, self.config["gmail"]["secrets_file"])
        # The file token.json stores the user's access and refresh tokens
        if os.path.exists(token):
            self.creds = Credentials.from_authorized_user_file(
                token, self.config["gmail"]["scopes"]
            )
        # If there are no (valid) credentials available, let the user log in.
        if not self.creds or not self.creds.valid:
            flow = InstalledAppFlow.from_client_secrets_file(
                secrets, self.config["gmail"]["scopes"]
            )
            self.creds = flow.run_local_server(port=0)
        else:
            self.creds.refresh(Request())
        # Write new token
        with open(token, "w") as f:
            f.write(self.creds.to_json())
        self.logger.info("connected")

        # Add database connection
        self.txv_db = TarxivDB(config_dir)

    def get_entry(self, objname):
        # Wait to avoid rate limiting
        time.sleep(self.config["tns"]["rate_limit"])
        # Run request to TNS server
        get_url = self.site + "/api/get/object"
        headers = {"User-Agent": self.marker}
        obj_request = OrderedDict([
            ("objid", ""),
            ("objname", objname),
            ("photometry", "0"),
            ("spectra", "1"),
        ])
        get_data = {"api_key": self.api_key, "data": json.dumps(obj_request)}
        response = requests.post(get_url, headers=headers, data=get_data)
        # Log
        self.logger.debug({"action": "retreived", "objname": objname})
        return json.loads(response.text)["data"]["reply"]

    def download_bulk_tns(self):
        # Run request to TNS Server
        self.logger.info("pulling bulk tns objects")
        get_url = (
            self.site + "/system/files/tns_public_objects/tns_public_objects.csv.zip"
        )
        json_data = [
            ("api_key", (None, self.api_key)),
        ]
        headers = {"User-Agent": self.marker}
        response = requests.post(get_url, files=json_data, headers=headers)

        # Write to bytesio and convert to pandas
        with zipfile.ZipFile(io.BytesIO(response.content)) as myzip:
            data = myzip.read(name="tns_public_objects.csv")
        objlist = pd.read_csv(io.BytesIO(data), skiprows=[0])["name"].to_list()
        self.logger.info(f"querying {len(objlist):,} individual object names")

        # Get full TNS entries and update to database
        for objname in objlist:
            # Get entry info from TNS
            tns_entry = self.get_entry(objname)
            # Insert to couchbase
            self.txv_db.upsert(objname, tns_entry, "tns")

    def monitor_notices(self):
        # Connect
        service = build("gmail", "v1", credentials=self.creds)
        while True:
            # Call the Gmail API
            self.logger.debug({"action": "checking_messages"})
            results = (
                service.users()
                .messages()
                .list(userId="me", labelIds=["INBOX"], q="is:unread")
                .execute()
            )
            messages = results.get("messages", [])

            # Wait for new messages
            if not messages:
                time.sleep(5)
                continue

            # Process messages
            self.logger.debug({"action": "processing_messages", "new": len(messages)})
            for message in messages:
                self.process_message(service, message)

    def process_message(self, service, message):
        # Pull message from gmail
        msg = service.users().messages().get(userId="me", id=message["id"]).execute()
        headers = msg["payload"]["headers"]
        for hdr in headers:
            # Only process emails from TNS
            if hdr["name"] == "From" and self.config["tns"]["email"] in hdr["value"]:
                # Decode and parse message body for TNS onj names
                data = msg["payload"]["body"]["data"]
                byte_code = base64.urlsafe_b64decode(data)
                text = byte_code.decode("utf-8")
                soup = BeautifulSoup(text, features="html.parser")
                obj_list = [a.text for a in soup.find_all("a", href=True) if a.text]

                # Get full TNS entries and update to database
                for objname in obj_list:
                    # Get entry info from TNS
                    tns_entry = self.get_entry(objname)
                    # Insert to couchbase
                    self.txv_db.upsert(objname, tns_entry, "tns")

        # Mark as read
        service.users().messages().modify(
            userId="me", id=message["id"], body={"removeLabelIds": ["UNREAD"]}
        ).execute()
        self.logger.debug({"action": "message_read", "id": message["id"]})
