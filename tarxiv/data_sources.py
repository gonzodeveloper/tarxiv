"""Pull and process lightcurves"""
from .utils import TarxivModule
import atlasapiclient.client as atlas_client
from atlasapiclient.exceptions import ATLASAPIClientError

from pyasassn.client import SkyPatrolClient
from astropy.time import Time
from collections import OrderedDict

import pandas as pd
import requests
import zipfile
import json
import time
import io
import re
import os

class Survey(TarxivModule):
    """
    Base class to interact with a Tarxiv survey or data source.
    """
    def __init__(self, *args, **kwargs):
        """
        Read in data for survey sources from config directory
        """
        super().__init__(*args, **kwargs)

        # Read in schema sources
        schema_sources = os.path.join(self.config_dir, "sources.json")
        with open(schema_sources) as f:
            self.schema_sources = json.load(f)

        # Survey name map (could write better, but fuck it)
        self.survey_source_map = {
            "TNS": 0,
            "ATLAS": 2,
            "ZTF": 3,
            "ASAS-SN": 5,
            "SHERLOCK": 7,
            "MANGROVE": 8
        }

    def get_object(self, *args, **kwargs):
        """
        Query the survey for object at a given set of coordinates.
        Must return metadata dict containing at least one survey designation, and any additional meta
                e.g. {"identifiers" : [{"name": ATLAS25XX, "source": 3}, ...],
                     {"meta": {"redshift": {"value": 0.003, "source": 8},
                               "hostname": [{"value": "NCGXXXX", "source": 9},
                                            {"value": "2MASS XXXXX", "source": 10}]
                               ...}}
        Also return lightcurve dataframe with columns [mjd, mag, mag_err, limit, filter, unit, survey],
            mjd: modified julian date,
            mag: magnitude,
            mag_err: magnitude error,
            limit: 5-sigma limiting magnitude,
            filter: bandpass filter,
            unit: telescope or camera for given measurement (if survey only has one unit, use 'main')
            survey: survey name.
        :return: survey_meta; dict (None if no results), survey_lc; DataFrame (empty df if no results)
        """
        raise NotImplementedError("each survey must implement their own logic to get meta/lightcurve")

    def update_object_meta(self, obj_meta, survey_meta):
        """
        Update the object meta schema with data from the survey meta returned by get_object.
        :param obj_meta: existing object meta schema; dict
        :param survey_meta: survey meta returned from get_object; dict
        :return:updated object meta dictionary
        """
        # Only update if we get returned object
        if survey_meta is not None:
            # Append sources to schema
            for source in self.config[self.module]['associated_sources']:
                obj_meta["sources"].append(self.schema_sources[source])

            for field, meta in survey_meta.items():
                if type(meta) is list:
                    for item in meta:
                        obj_meta[field].append(item)
                else:
                    obj_meta[field].append(meta)

        return obj_meta


    def meta_add_peak_mags(self, obj_meta, obj_lc_df):
        """
        Once we have all the object dataframes collated; find peak mag for each filter and append to object_meta.
        :param obj_meta: object meta schema; dict
        :param obj_lc_df: light curve dataframe; pd.DataFrame
        :return:object_meta; updated object meta dictionary
        """

        # Get brightest mag for each filter
        filter_df = obj_lc_df.groupby('filter').min()

        peak_mags = []
        for filter_name, row in filter_df.iterrows():
            peak_mag = {"filter": filter_name,
                        "value": row["mag"],
                        "mjd_recorded": row["mjd"],
                        "source": self.survey_source_map[row["survey"]]}
            peak_mags.append(peak_mag)
        # Append if exists
        if peak_mags:
            obj_meta["peak_mag"] = peak_mags

        return obj_meta


class ASAS_SN(Survey):
    """
    Interface to ASAS-SN SkyPatrol.
    """

    def __init__(self, *args, **kwargs):
        """
        Connect to ASAS-SN SkyPatrol API.
        """
        super().__init__("asas-sn", *args, **kwargs)

        # Also need ASAS-SN client
        self.client = SkyPatrolClient(verbose=False)

    def get_object(self, ra_deg, dec_deg, radius=15):
        """
        Get ASAS-SN Lightcurve curve from coordinates using cone_search.
        :param ra_deg: right ascension in degrees; float
        :dec_deg: declination in degrees; float
        :param radius: radius in arcseconds; int
        return asas-sn metadata and lightcurve dataframe
        """
        # Query client
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

        # Get meta
        nearest = lcs.catalog_info.iloc[0]
        nearest_id = nearest['asas_sn_id']
        meta = {'identifiers': [{"name": str(nearest_id), 'source': 6}]}
        # Sometimes we have meta but no database object (will fix later)
        if len(lcs.data) == 0:
            return meta, pd.DataFrame()

        # Get LC
        lc_df =  lcs[nearest_id].data
        lc_df['mjd'] = lc_df.apply(lambda row: Time(row['jd'], format='jd').mjd, axis=1)
        lc_df.rename({"phot_filter": "filter", "camera": "unit"}, axis=1, inplace=True)
        # Do not return data from bad images
        lc_df = lc_df[lc_df['quality'] != "B"]
        # Throw out non_detections if not specified
        lc_df = lc_df[lc_df['mag_err'] < 99]
        lc_df['survey'] = "ASAS-SN"
        return meta, lc_df[['mjd', 'mag', 'mag_err', 'limit', 'filter', 'unit', 'survey']]


class ZTF(Survey):
    """
    Interface to ZTF Fink broker.
    """
    def __init__(self, *args, **kwargs):
        super().__init__("ztf", *args, **kwargs)

    def get_object(self, ra_deg, dec_deg, radius=15):
        """
        Get ZTF Lightcurve from coordinates using cone_search.
        :param ra_deg: right ascension in degrees; float
        :dec_deg: declination in degrees; float
        :param radius: radius in arcseconds; int
        return ztf metadata and lightcurve dataframe
        """
        result = requests.post(
            f"{self.config['fink_url']}/api/v1/conesearch",
            json={"ra": ra_deg, "dec": dec_deg, "radius": radius, "columns": "i:objectId"},
        )
        # check status
        if result.status_code != 200:
            self.logger.warning({"status": "fink_error", "http_code": result.status_code})
            return None, pd.DataFrame()
        # get data for the match
        matches = [val["i:objectId"] for val in result.json()]
        ztf_name = matches[0]

        if len(matches) == 0:
            self.logger.info({"status": "fink_cone_search_miss"})
            return None, pd.DataFrame()

        # Query
        result = requests.post(
            f"{self.config['fink_url']}/api/v1/objects",
            json={"objectId": ztf_name, "output-format": "json"}
        )
        # check status
        if result.status_code != 200:
            self.logger.warning({"status": "fink_error", "http_code": result.status_code})
            return None, pd.DataFrame()
        # check returns
        if result.json() == []:
            self.logger.info({"status": "fink_ztf_id_miss"})
            return None, pd.DataFrame()

        # Metadata on each line of photometry, we only take first row (d prefix are non-phot)
        result_meta = result.json()[0]
        meta = {"identifiers": [{"name": ztf_name, "source": 3}]}
        meta['host_name'] = []
        if result_meta["d:mangrove_2MASS_name"] != 'None':
            host_name = {"name": result_meta["d:mangrove_2MASS_name"], "source": 8}
            meta['host_name'].append(host_name)
        if result_meta["d:mangrove_HyperLEDA_name"] != 'None':
            host_name = {"name": result_meta["d:mangrove_HyperLEDA_name"], "source": 8}
            meta['host_name'].append(host_name)
        if len(meta['host_name']) == 0:
            del meta['host_name']

        # Lightcurve columns and values
        cols = {
            "i:magpsf": "mag",
            "i:sigmapsf": "mag_err",
            "i:fid": "filter",
            "i:jd": "jd",
            "i:diffmaglim": "limit"
            }
        filter_map = {'1': 'g', '2': 'R', '3': 'i'}
        # Push into DataFrame
        lc_df = pd.read_json(io.BytesIO(result.content))
        lc_df = lc_df.rename(cols, axis=1)
        lc_df = lc_df[list(cols.values())]
        lc_df['mjd'] = lc_df.apply(lambda row: Time(row['jd'], format='jd').mjd, axis=1)
        lc_df["filter"] = lc_df['filter'].astype(str).map(filter_map)
        # JD now unneeded
        lc_df.drop('jd', axis=1, inplace=True)
        # Add unit/survey columns
        lc_df["unit"] = "main"
        lc_df["survey"] = "ZTF"

        return meta, lc_df

class ATLAS(Survey):
    """
    Interface to ATLAS Transient Web Server.
    """
    def __init__(self, *args, **kwargs):
        super().__init__("atlas", *args, **kwargs)

    def get_object(self, ra_deg, dec_deg, radius=15):
        """
        Get ZTF Lightcurve from coordinates using cone_search.
        :param ra_deg: right ascension in degrees; float
        :dec_deg: declination in degrees; float
        :param radius: radius in arcseconds; int
        return ztf metadata and lightcurve dataframe
        """
        try:
            # First run cone search to get id
            cone_res = atlas_client.ConeSearch(api_config_file=self.config_file,
                                     payload={"ra": ra_deg,
                                              "dec": dec_deg,
                                              "radius": radius,
                                              "requestType": "nearest"},
                                     get_response=True)
        except ATLASAPIClientError:
            return None, pd.DataFrame()

        # Get atlas id and query for data
        try:
            atlas_id = cone_res.response_data['object']  # The ATLAS is from cone search

            # Get light curve
            curve_res = atlas_client.RequestSingleSourceData(api_config_file=self.config_file,
                                                         atlas_id=str(atlas_id),
                                                         get_response=True)
        except ATLASAPIClientError:
            return None, pd.DataFrame()

        # Contains meta and lc
        result = curve_res.response_data[0]

        # Insert meta data
        meta = {"identifiers": [{"name": result["object"]["id"], "source": 1}]}
        if result["object"]["atlas_designation"] is not None:
            atlas_name = {"name": result["object"]["atlas_designation"], "source": 2}
            meta["identifiers"].append(atlas_name)
        # Add sherlock crossmatch if exists
        if result["sherlock_crossmatches"]:
            result['sherlock'] = result['sherlock_crossmatches'][0]
            if result["sherlock"]["z"] is not None:
                meta["redshift"] = {"value": result["sherlock"]["z"], "source": 7}

        # DETECTIONS
        lc_df = pd.DataFrame(result['lc'])[['mjd', 'mag', 'magerr', 'mag5sig', 'filter', "expname"]]
        lc_df.columns = ['mjd', 'mag', 'mag_err', 'limit', 'filter', "expname"]
        #lc_df['upperlimit'] = False
        # NONDETECTIONS (leave out for now)
        # lc_df_non = pd.DataFrame(result['lcnondets'])[['mjd', 'mag5sig', 'input', 'filter', "expname"]]
        # lc_df_non.columns = ['mjd', 'mag', 'mag_err', 'filter', "expname"]
        # lc_df_non['mag_err'] = np.nan
        # lc_df_non['upperlimit'] = True
        # Concat
        # lc_df = pd.concat((lc_df, lc_df_non))
        # Add a column to record which ATLAS unit the value was taken from
        lc_df['unit'] = lc_df["expname"].str[:3]
        lc_df.drop('expname', axis=1, inplace=True)
        lc_df['survey'] = "ATLAS"

        return meta, lc_df

class TNS(Survey):
    """
    Interface to Transient Name Server API.
    """

    def __init__(self, *args, **kwargs):
        """
        Read in credentials and construct 'marker' for API calls
        """
        super().__init__("tns", *args, **kwargs)

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

    def get_object(self, objname):
        """
        Get TNS metadata for a given object name.
        :param objname: TNS object name, e.g., 2025xxx; str
        :return: metadata dictionary and empty dataframe (since we are not pulling lightcurve)
        """
        # Wait to avoid rate limiting
        time.sleep(self.config["tns"]["rate_limit"])
        # Run request to TNS server
        get_url = self.site + "/api/get/object"
        headers = {"User-Agent": self.marker}
        obj_request = OrderedDict([
            ("objid", ""),
            ("objname", objname),
            ("photometry", "0"),
            ("spectra", "0"),
        ])
        get_data = {"api_key": self.api_key, "data": json.dumps(obj_request)}
        response = requests.post(get_url, headers=headers, data=get_data)
        # Meta
        result = json.loads(response.text)["data"]
        # Reduce meta to what we want
        meta = {
            "identifiers": {"name": result["objname"], "source": 0},
            "ra_deg": {"value": result["radeg"], "source": 0},
            "dec_deg": {"value": result["decdeg"], "source": 0},
            "ra_hms": {"value": result["ra"], "source": 0},
            "dec_dms": {"value": result["dec"], "source": 0},
            "object_type": [{"value": result["name_prefix"], "source": 0},
                         {"value": result["object_type"]["name"], "source": 0}],
            "discovery_date": {"value": result["discoverydate"], "source": 0},
            "reporting_group": {"value": result["reporting_group"]['group_name'], "source": 0},
            "discovery_data_source": {"value": result["discovery_data_source"]['group_name'], "source": 0},
        }
        if result["redshift"] is not None:
            meta["redshift"] = {"value": result["redshift"], "source": 0}
        if result["hostname"] is not None:
            meta["host_name"] = {"value": result["hostname"], "source": 0}

        # TNS only returns meta
        return meta, pd.DataFrame()

    def download_bulk_tns(self):
        """
        Download bulk TNS public object csv and convert to dataframe.
        Used for bulk back-processing of TNS sources
        :return: full TNS public object dataframe
        """
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

        return pd.read_csv(io.BytesIO(data), skiprows=[0])


if __name__ == "__main__":
    """Execute the test suite"""
    import sys
    import doctest

    sys.exit(doctest.testmod()[0])
