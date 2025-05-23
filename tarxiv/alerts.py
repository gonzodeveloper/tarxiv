# Listen for new TNS Alerts
from .utils import TarxivModule
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build

from queue import Queue, Empty
from bs4 import BeautifulSoup
import threading
import base64
import time
import os


class Gmail(TarxivModule):
    """Module for interfacing with gmail and parsing TNS alerts."""

    def __init__(self, *args, **kwargs):
        """
        Create module, authenticate gmail and establish connection.
        """
        super().__init__("gmail", *args, **kwargs)

        # Logging
        self.logger.info({"status": "connecting"})
        # Get gmail token
        self.creds = None
        # Absolute paths
        token = os.path.join(self.config_dir, self.config["gmail"]["token_name"])
        secrets = os.path.join(self.config_dir, self.config["gmail"]["secrets_file"])
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
        # Connect to service
        self.service = build("gmail", "v1", credentials=self.creds)
        # Connect to email
        self.logger.info({"status": "connected"})

        # Create thread value
        self.t = None
        # Create internal queue
        self.q = Queue()
        # Create stop flag for monitoring
        self.stop_event = threading.Event()

    def poll(self, timeout=1):
        """
        Once we have began monitoring notices, poll the queue for new messages and alerts
        :param timeout, seconds; int
        :return: poll result tuple containing the original message and a list of tns object names; (message, alerts)
                 if there is nothing in the queue then poll will return None after the timeout has expired.
        """
        try:
            result = self.q.get(block=True, timeout=timeout)
        except Empty:
            result = None

        return result


    def parse_message(self, message, service):
        """
        Parse a gmail message for tns object names
        :param message: gmail message object
        :param service: gmail service object
        :return: list of tns object names
        """
        # Result stays non of message is not structured properly or not from TNS
        result = None

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
                result = obj_list

        return result

    def mark_read(self, message, verbose=False):
        """
        Marks message as read in gmail, so it won't show up again in our monitoring stream
        :param message: gmail message object
        :return: void
        """
        # Mark as read
        self.service.users().messages().modify(
            userId="me", id=message["id"], body={"removeLabelIds": ["UNREAD"]}
        ).execute()
        status = {"action": "message_read", "id": message["id"]}
        if verbose:
            self.logger.info(status)
        else:
            self.logger.debug(status)

    def monitor_notices(self):
        """
        Starts thread to monitor gmail account for tns alerts:
        :return: void
        """
        self.t = threading.Thread(target=self._monitoring_thread, daemon=True)
        self.t.start()
        # Log
        self.logger.info({"status":"starting monitoring thread"})

    def stop_monitoring(self):
        """
        Kill monitoring thread.
        :return: void
        """
        if self.t is not None:
            # Set the stop event (should kill the thread)
            self.stop_event.set()
            self.t.join()
        # Log
        self.logger.info({"status":"stopping monitoring thread"})
    def _monitoring_thread(self):
        """
        Open a gmail service object and continuously monitor gmail for new messages.
        Each new message is parsed of tns object alerts and results are submitted to local queue.
        Also refresh the token every 30 minutes.
        :return: void
        """
        # Connect to service
        service = build("gmail", "v1", credentials=self.creds)
        last_refresh = time.time()
        while not self.stop_event.is_set():
            now = time.time()
            if now - last_refresh >= (30 * 60):
                self.creds.refresh(Request())
                service = build("gmail", "v1", credentials=self.creds)
                last_refresh = now

            # Call the Gmail API
            self.logger.debug({"action": "checking_messages"})
            results = (
                service.users()
                .messages()
                .list(userId="me", labelIds=["INBOX"], q="is:unread")
                .execute()
            )
            messages = results.get("messages", [])

            if not messages:
                time.sleep(self.config["gmail"]["polling_interval"])
                continue

            for message in messages:
                # Parse message for tns alerts
                alerts = self.parse_message(message, service)

                if alerts is None:
                    self.mark_read(message)
                    continue
                # Log
                self.logger.debug({"status": "recieved alerts", "objects": alerts})

                # Submit to queue for processing
                self.q.put((message, alerts))
