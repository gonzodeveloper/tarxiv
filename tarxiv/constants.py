import os

FINKAPIURL = "https://fink-portal.org"
ATLASAPIURL = "https://star.pst.qub.ac.uk/sne/atlas4/api/"


# ATLAS token provided by admin
atlas_token = os.environ["ATLASAPI_CONFIG"]
atlas_headers = {"Authorization": f"Token {atlas_token}"}


atlas2tarxiv_dic = {
                            "object:ra": "ra",
                            "object:dec": "dec",
                            "lc:mag": "photometry:magnitude",
                            "lc:magerr": "photometry:e_magnitude",
                            "lc:mjd": "photometry:time",
                            "lc:filter": "photometry:band",
                        }
