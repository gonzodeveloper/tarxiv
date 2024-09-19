from tarxiv.utils import read_config

from couchbase.cluster import Cluster, ClusterOptions
from couchbase.auth import PasswordAuthenticator
import pandas as pd
import requests
import zipfile
import uuid
import json
import sys
import io

# Read in config
config = read_config(sys.argv[1])

# Create marker
tns_marker = {
        "tns_id": config["tns"]["id"], 
        "type": config["tns"]["type"], 
        "name": config["tns"]["name"]
        }
tns_marker_str = "tns_marker" + json.dumps(tns_marker, separators=(',', ':'))

# Use fink to download
json_data = [('api_key', (None, config["tns"]["api_key"])),]

# define header
headers = {'User-Agent': tns_marker_str}

r = requests.post(
      config["tns"]["site"] + '/system/files/tns_public_objects/tns_public_objects.csv.zip',
      files=json_data,
      headers=headers
)

# Write to bytesio and convert to pandas
with zipfile.ZipFile(io.BytesIO(r.content)) as myzip:
    data = myzip.read(name='tns_public_objects.csv')

tns_df = pd.read_csv(io.BytesIO(data), skiprows=[0])

# Sample for testing 
tns_df = tns_df.sample(1000)

# Connect to couchbase
connection_str = 'couchbase://' + config["database"]["host"]
options = ClusterOptions(PasswordAuthenticator(
    config["database"]["user"], config["database"]["pass"]))
cluster = Cluster(options)
# Get tarxiv access
cb = cluster.bucket('tarxiv')
coll = cb.collection('tns')

# Query all TNS objects in database
for idx, tns_row in tns_df.iterrows():
    print(f"Retreiving {idx:06d} of {len(tns_df):06d")
    # Run request to TNS server
    get_url = TNS_URL +  "/api/get/object"
    headers = {'User-Agent': tns_marker}
    obj_request = OrderedDict(
            ("objname", tns_row['name']), 
            ("objid", ""), 
            ("photometry", "0"), 
            ("spectra", "1"))
    get_data = {'api_key': tns_api_key, 'data': json.dumps(obj_request)}
    response = requests.post(get_url, headers = headers, data = get_data)
    payload = json.loads(response.text)["data"]["reply"]
    # Upsert to database
    doc_id = str(uuid.uuid4())
    coll.upsert(doc_id, payload)

print()
cluster.close()






