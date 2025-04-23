import os

FINKAPIURL = "https://api.fink-portal.org"
ATLASAPIURL = "https://star.pst.qub.ac.uk/sne/atlas4/api/"


# ATLAS token provided by admin
atlas_token = os.environ["ATLASAPI_CONFIG"]
atlas_headers = {"Authorization": f"Token {atlas_token}"}
