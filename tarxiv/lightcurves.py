"""Pull and process lightcurves"""
import atlasapiclient.client as atlas_client
from pyasassn.client import SkyPatrolClient
from astropy.time import Time
from pprint import pprint
import pandas as pd
import numpy as np
import requests
import logging
import yaml
import json
import sys
import io
import re

class LightCurves:

    def __init__(self, config_file, debug=False):
        # Read in config
        self.config_file = config_file
        with open(config_file) as stream:
            self.config = yaml.safe_load(stream)
        # Read in schema sources
        with open(self.config["schema_sources"]) as f:
            self.schema_sources = json.load(f)

        # Logger
        self.logger = logging.getLogger("tarxiv_lightcurves")
        # Set log level
        if debug:
            self.logger.setLevel(logging.DEBUG)
        else:
            self.logger.setLevel(logging.INFO)
        # Print log to stdout
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)

        #
        # Get asas-sn client
        self.client = SkyPatrolClient(verbose=False)


    def get_transient(self, tns_meta, radius=15, non_detections=False):
        # First pull data from light curve servers
        asas_sn_meta, asas_sn_lc = self.get_asas_sn_lc(tns_meta['radeg'], tns_meta['decdeg'], radius=radius, non_detections=non_detections)
        atlas_meta, atlas_lc = self.get_atlas_lc(tns_meta['radeg'], tns_meta['decdeg'], radius=radius, non_detections=non_detections)
        ztf_meta, ztf_lc = self.get_ztf_lc(tns_meta['radeg'], tns_meta['decdeg'], radius=radius, non_detections=non_detections)

        # Basic Object Schema
        obj_meta = {
            "schema": "https://github.com/astrocatalogs/schema/README.md",
            # Only give TNS as source for start
            "sources": [self.schema_sources['tns']],
            "identifiers": [{
                "id":tns_meta["name"],
                "source": 0,
            }],
            "ra_deg": {
                "value": tns_meta["raradeg"],
                "u_value": "degrees",
                   "source": 0},
            "dec_deg": {
                "value": tns_meta["decdeg"],
                "u_value": "degrees",
                    "source": 0},
            "ra_hms": {
                "value": tns_meta["ra"],
                "u_value": "hour angle HH:MM:SS.S",
                "source": 0
            },
            "dec_dms": {
                "value": tns_meta["dec"],
                "u_value": "degrees DD:MM:SS.SS",
                "source": 0
            },
            "object_type": {
                "value": tns_meta["object_type"]["name"],
                "source": 0
            },
            "source_type": {
                "value": tns_meta["name_prefix"],
                "source": 0
            },
            "discovery_date":{
                "value": tns_meta["discoverydate"],
                "source": 0
            },
            "reporting_group": {
                "value": tns_meta["reporting_group"]["group_name"],
                "source": 0
            },
            "discovery_data_source": {
                "value": tns_meta["discovery_data_source"]["group_name"],
                "source": 0
            },
            "redshifts": [],
            "hostnames": [],
        }
        # Add redshift and host name if they exist in TNS
        if tns_meta['redshifts'] is not None:
            redshift = {"value": tns_meta['redshift'], "source": 0}
            obj_meta['redshifts'].append(redshift)
        if tns_meta['hostnames'] is not None:
            host_name = {"name": tns_meta['host_name'], "source": 0}
            obj_meta['hostnames'].append(host_name)

        # Start lightcurve
        obj_lc = []

        if atlas_meta is not None:
            # Append ATLAS as source
            obj_meta["sources"].append(self.schema_sources['atlas_twb'])
            obj_meta["sources"].append(self.schema_sources['atlas_survey'])
            # Add internal id and atlas name
            internal_id = {"id": atlas_meta['id'], "source": 1}
            atlas_name = {"id": atlas_meta['atlas_designation'], "source": 2}
            obj_meta['identifiers'].append(internal_id)
            obj_meta['identifiers'].append(atlas_name)
            # Add redshift if recorded
            if atlas_meta["sherlock"]["z"] is not None:
                obj_meta["sources"].append(self.schema_sources['sherlock'])
                redshift = {"value": atlas_meta["sherlock"]["z"], "source": 7}
                obj_meta["redshifts"].append(redshift)
            # FEEL FREE TO ADD MORE META HERE
            # Append lightcurve
            lc_json = atlas_lc.to_dict(orient='records')
            for item in lc_json:
                obj_lc.append(item)

        if ztf_meta is not None:
            # Append ZTF/Fink as data sources
            obj_meta["sources"].append(self.schema_sources['ztf_survey'])
            obj_meta["sources"].append(self.schema_sources['fink'])
            # Add ids
            ztf_id = {"id": ztf_meta['ztf_name'], "source": 3}
            obj_meta['identifiers'].append(ztf_id)
            # Add MANGROVE cross matches if recorded
            mangrove_added = False
            if ztf_meta["d:mangrove_2MASS_name"] != 'None':
                obj_meta["sources"].append(self.schema_sources['mangrove'])
                mangrove_added = True
                host_name = {"name": ztf_meta["d:mangrove_2MASS_name"], "source": 7}
                obj_meta["hostnames"].append(host_name)
            if ztf_meta["d:mangrove_HyperLEDA_name"] != 'None':
                if mangrove_added is False:
                    obj_meta["sources"].append(self.schema_sources['mangrove'])
                host_name = {"name": ztf_meta["d:mangrove_HyperLEDA_name"], "source": 7}
                obj_meta["hostnames"].append(host_name)
            # FEEL FREE TO ADD MORE META HERE
            # Append lightcurve
            lc_json = ztf_lc.to_dict(orient='records')
            for item in lc_json:
                obj_lc.append(item)

        if asas_sn_meta is not None:
            # Append ASAS-SN/Skypatrol as sources
            obj_meta["sources"].append(self.schema_sources['asas-sn_survey'])
            obj_meta["sources"].append(self.schema_sources['asas-sn_skypatrol'])
            # Add id
            skypatrol_id = {"id": asas_sn_meta['asas_sn_id'], "source": 6}
            obj_meta['identifiers'].append(skypatrol_id)
            # Append lightcurve
            lc_json = asas_sn_lc.to_dict(orient='records')
            for item in lc_json:
                obj_lc.append(item)

    def get_asas_sn_lc(self, ra_deg, dec_deg, radius=15, non_detections=False):
        """
        Get ASAS-SN Lightcurve curve from coordinates using cone_search.
        """
        query = f"WITH sources AS                " \
                f"  (                            " \
                f"      SELECT                   " \
                f"          asas_sn_id,          " \
                f"          ra_deg,              " \
                f"          dec_deg,             " \
                f"          catalog_sources,     " \
                f"          DISTANCE(ra_deg, dec_deg, {ra_deg}, {dec_deg}) AS angular_dist " \
                f"     FROM master_list          " \
                f"  )                            " \
                f"SELECT *                       " \
                f"FROM sources                   " \
                f"WHERE angular_dist <= ARCSEC({radius}) " \
                f"ORDER BY angular_dist ASC      "

        query = re.sub(r'(\s+)', ' ', query)
        lcs = self.client.adql_query(query, download=True)

        # Get nearest
        nearest = lcs.catalog_info.iloc[0]
        nearest_id = nearest['asas_sn_id']
        nearest_dist = nearest['angular_dist']
        catalog_sources = {catalog: self.client.query_list(int(nearest_id), catalog=catalog, id_col='asas_sn_id')['name'].to_list()[0]
                           for catalog in nearest['catalog_sources']}
        meta = {'asas_sn_id': nearest_id, 'cross_match_distance': nearest_dist, 'catalog_sources': catalog_sources}
        lc_df =  lcs[nearest_id].data
        lc_df['mjd'] = lc_df.apply(lambda row: Time(row['jd'], format='jd').mjd, axis=1)
        lc_df.rename({"phot_filter": "filter"}, axis=1, inplace=True)

        # Do not return data from bad images
        lc_df = lc_df[lc_df['quality'] != "B"]
        # Throw out non_detections if not specified
        if non_detections is False:
            lc_df = lc_df[lc_df['mag_err'] < 99]

        lc_df.rename(columns={'limit': 'zp'}, inplace=True)
        return meta, lc_df[['mjd', 'mag', 'mag_err', 'filter', 'zp']]


    def get_ztf_lc(self, ra_deg, dec_deg, radius=15, non_detections=False):
        result = requests.post(
            f"{self.config['fink_url']}/api/v1/conesearch",
            json={"ra": ra_deg, "dec": dec_deg, "radius": radius, "columns": "i:objectId"},
        )
        # check status
        if result.status_code != 200:
            self.logger.warning({"status": "fink_error", "http_code": result.status_code})
            return None

        # get data for the match
        matches = [val["i:objectId"] for val in result.json()]
        ztf_name = matches[0]

        if len(matches) == 0:
            self.logger.info({"status": "fink_cone_search_miss"})
            return None

        # Query
        result = requests.post(
            f"{self.config['fink_url']}/api/v1/objects",
            json={"objectId": ztf_name, "output-format": "json"}
        )

        # check status
        if result.status_code != 200:
            self.logger.warning({"status": "fink_error", "http_code": result.status_code})
            return None

        # check returns
        if result.json() == []:
            self.logger.info({"status": "fink_ztf_id_miss"})
            return None

        # Metadata on each line of photometry, we only take first row (d prefix are non-phot)
        meta = {k : v for k, v in result.json()[0].items() if k[0] == 'd'}
        # Drop lc features
        del meta['d:lc_features_g'], meta['d:lc_features_r']
        # Also include original ZTF name
        meta["name"] = ztf_name

        # Lightcurve columns and values
        cols = {
            "i:magpsf": "mag",
            "i:sigmapsf": "mag_err",
            "i:fid": "filter",
            "i:jd": "jd",
            "i:magzpsci": "zp"
            }
        filter_map = {'1': 'g', '2': 'R', '3': 'i'}
        lc_df = pd.read_json(io.BytesIO(result.content))
        lc_df = lc_df.rename(cols, axis=1)
        lc_df = lc_df[list(cols.values())]
        lc_df['mjd'] = lc_df.apply(lambda row: Time(row['jd'], format='jd').mjd, axis=1)
        lc_df["filter"] = lc_df['filter'].astype(str).map(filter_map)
        return meta, lc_df


    def get_atlas_lc(self, ra_deg, dec_deg, radius=15, non_detections=False):
        # First run cone search to get id
        cone = atlas_client.ConeSearch(api_config_file=self.config_file,
                                 payload={"ra": ra_deg,
                                          "dec": dec_deg,
                                          "radius": radius,
                                          "requestType": "nearest"},
                                 get_response=True)
        atlas_id = cone.response_data['object']  # The ATLAS is from cone search

        # Get light curve
        curve = atlas_client.RequestSingleSourceData(api_config_file=self.config_file,
                                                     atlas_id=str(atlas_id),
                                                     get_response=True)
        meta = curve.response_data[0]['object']
        meta['sherlock'] = curve.response_data[0]['sherlock_crossmatches'][0]

        lc_json = curve.response_data[0]['lc']


        # DETECTIONS
        lc_df = pd.DataFrame(lc_json)[['mjd', 'mag', 'magerr', 'filter', 'zp']]
        lc_df.columns = ['mjd', 'mag', 'mag_err', 'filter', 'zp']
        #lc_df['upperlimit'] = False

        # NONDETECTIONS
        if non_detections:
            lc_df_non = pd.DataFrame(lc_json['lcnondets'])[['mjd', 'mag5sig', 'input', 'filter', 'expname', ]]
            lc_df_non.columns = ['mjd', 'mag', 'mag_err', 'filter', 'zp']
            lc_df_non['mag_err'] = np.nan
            #lc_df_non['upperlimit'] = True

            # sort by MJD
            lc_df = pd.concat((lc_df, lc_df_non))

        # adds a column to record which ATLAS unit the value was taken from
        lc_df['unit'] = lc_df.telescope.apply(lambda x: x[:3]).values
        lc_df['survey'] = "ATLAS"


        return meta, lc_df


if __name__ == "__main__":
    """Execute the test suite"""
    import sys
    import doctest

    sys.exit(doctest.testmod(verbosity=True)[0])
