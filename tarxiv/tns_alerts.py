# Listen for new TNS Alerts
from collections import OrderedDict
import requests
import json


def generate_marker(tns_info):
    # Create marker
    tns_marker = {
        "tns_id": tns_info["id"],
        "type": tns_info["type"],
        "name": tns_info["name"],
    }
    return "tns_marker" + json.dumps(tns_marker, separators=(",", ":"))


def get_tns_entry(objid, tns_info):
    # Run request to TNS server
    get_url = tns_info["site"] + "/api/get/object"
    headers = {"User-Agent": generate_marker(tns_info)}
    obj_request = OrderedDict([
        ("objid", objid),
        ("objname", ""),
        ("photometry", "0"),
        ("spectra", "1"),
    ])
    get_data = {"api_key": tns_info["api_key"], "data": json.dumps(obj_request)}
    response = requests.post(get_url, headers=headers, data=get_data)

    return json.loads(response.text)["data"]["reply"]
