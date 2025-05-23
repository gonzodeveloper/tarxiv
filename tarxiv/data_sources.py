"""Pull and process lightcurves"""
from .utils import TarxivModule, SurveyMetaMissing, SurveyLightCurveMissing
from atlasapiclient.exceptions import ATLASAPIClientError
import atlasapiclient.client as atlas_client

from pyasassn.client import SkyPatrolClient
from collections import OrderedDict
from astropy.time import Time

import pandas as pd
import requests
import traceback
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
        if len(obj_lc_df) == 0:
            return pd.DataFrame()
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

    def get_object(self, obj_name, ra_deg, dec_deg, radius=15):
        """
        Get ASAS-SN Lightcurve curve from coordinates using cone_search.
        :param obj_name: name of object (used for logging); str
        :param ra_deg: right ascension in degrees; float
        :dec_deg: declination in degrees; float
        :param radius: radius in arcseconds; int
        return asas-sn metadata and lightcurve dataframe
        """
        # Set meta and lc_df empty to start
        meta , lc_df = None, pd.DataFrame()
        # Initial status
        status = {"obj_name": obj_name}

        try:
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
            if lcs is None:
                raise SurveyMetaMissing
            # Get meta
            nearest = lcs.catalog_info.iloc[0]
            nearest_id = nearest['asas_sn_id']
            meta = {'identifiers': [{"name": str(nearest_id), 'source': 6}]}
            # Log
            status.update({"status": "match", "id": nearest_id})
            # Sometimes we have meta but no database object (will fix later)
            if lcs.data is None or len(lcs.data) == 0:
                raise SurveyLightCurveMissing
            # Get LC
            lc_df =  lcs[nearest_id].data
            lc_df['mjd'] = lc_df.apply(lambda row: Time(row['jd'], format='jd').mjd, axis=1)
            lc_df.rename({"phot_filter": "filter", "camera": "unit"}, axis=1, inplace=True)
            # Do not return data from bad images
            lc_df = lc_df[lc_df['quality'] != "B"]
            # Throw out non_detections if not specified
            lc_df = lc_df[lc_df['mag_err'] < 99]
            lc_df['survey'] = "ASAS-SN"
            lc_df = lc_df[['mjd', 'mag', 'mag_err', 'limit', 'filter', 'unit', 'survey']]
            # Update
            status["lc_count"] = len(lc_df)
        except SurveyMetaMissing:
            status['status'] = "no match"
        except SurveyLightCurveMissing:
            status["status"].append("|no light curve")
        except Exception as e:
            status.update({"status": "encontered unexpected error",
                           "message": str(e),
                           "details": traceback.format_exc()})
        finally:
            self.logger.info(status)
            return meta, lc_df


class ZTF(Survey):
    """
    Interface to ZTF Fink broker.
    """
    def __init__(self, *args, **kwargs):
        super().__init__("ztf", *args, **kwargs)

    def get_object(self, obj_name, ra_deg, dec_deg, radius=15):
        """
        Get ZTF Lightcurve from coordinates using cone_search.
        :param obj_name: name of object (used for logging); str
        :param ra_deg: right ascension in degrees; float
        :dec_deg: declination in degrees; float
        :param radius: radius in arcseconds; int
        return ztf metadata and lightcurve dataframe
        """
        # Set meta and lc_df empty to start
        meta , lc_df = None, pd.DataFrame()
        # Initial status
        status = {"obj_name": obj_name}
        try:
            # Hit FINK API
            result = requests.post(
                f"{self.config['fink_url']}/api/v1/conesearch",
                json={"ra": ra_deg, "dec": dec_deg, "radius": radius, "columns": "i:objectId"},
            )
            # check status
            if result.status_code != 200:
                raise SurveyMetaMissing

            # get data for the match
            matches = [val["i:objectId"] for val in result.json()]

            if len(matches) == 0:
                raise SurveyMetaMissing

            # Show ztf name
            ztf_name = matches[0]
            status.update({"status": "match", "id": ztf_name})

            # Query
            result = requests.post(
                f"{self.config['fink_url']}/api/v1/objects",
                json={"objectId": ztf_name, "output-format": "json"}
            )
            # check status
            if result.status_code != 200 or result.json() == []:
                raise SurveyLightCurveMissing

            # Metadata on each line of photometry, we only take first row (d prefix are non-phot)
            result_meta = result.json()[0]
            meta = {"identifiers": [{"name": ztf_name, "source": 3}]}
            meta['host_name'] = []
            if "d:mangrove_2MASS_name" in result_meta.keys() and result_meta["d:mangrove_2MASS_name"] != 'None':
                host_name = {"name": result_meta["d:mangrove_2MASS_name"], "source": 8}
                meta['host_name'].append(host_name)
            if "d:mangrove_2MASS_name" in result_meta.keys() and result_meta["d:mangrove_HyperLEDA_name"] != 'None':
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
            status["lc_count"] = len(lc_df)

        except SurveyMetaMissing:
            status['status'] = "no match"
        except SurveyLightCurveMissing:
            status["status"].append("|no light curve")

        except Exception as e:
            status.update({"status": "encontered unexpected error",
                           "message": str(e),
                           "details": traceback.format_exc()})
        finally:
            self.logger.info(status)
            return meta, lc_df


class ATLAS(Survey):
    """
    Interface to ATLAS Transient Web Server.
    """
    def __init__(self, *args, **kwargs):
        super().__init__("atlas", *args, **kwargs)

    def get_object(self, obj_name, ra_deg, dec_deg, radius=15):
        """
        Get ZTF Lightcurve from coordinates using cone_search.
        :param obj_name: name of object (used for logging); str
        :param ra_deg: right ascension in degrees; float
        :dec_deg: declination in degrees; float
        :param radius: radius in arcseconds; int
        return ztf metadata and lightcurve dataframe
        """
        # Set meta and lc_df empty to start
        meta , lc_df = None, pd.DataFrame()
        # Initial status
        status = {"obj_name": obj_name}
        try:
            # First run cone search to get id
            cone_res = atlas_client.ConeSearch(api_config_file=self.config_file,
                                     payload={"ra": ra_deg,
                                              "dec": dec_deg,
                                              "radius": radius,
                                              "requestType": "nearest"},
                                     get_response=True)


            # Get atlas id and query for data
            atlas_id = cone_res.response_data['object']  # The ATLAS is from cone search
            # Get light curve
            curve_res = atlas_client.RequestSingleSourceData(api_config_file=self.config_file,
                                                             atlas_id=str(atlas_id),
                                                             get_response=True)
            # Contains meta and lc
            result = curve_res.response_data[0]
            status.update({"status": "match", "id": result["object"]["id"]})

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

            # Add a column to record which ATLAS unit the value was taken from
            lc_df['unit'] = lc_df["expname"].str[:3]
            lc_df.drop('expname', axis=1, inplace=True)
            lc_df['survey'] = "ATLAS"

        except (SurveyMetaMissing, ATLASAPIClientError):
            status['status'] = "no match"
        except SurveyLightCurveMissing:
            status["status"].append("|no light curve")

        except Exception as e:
            status.update({"status": "encontered unexpected error",
                           "message": str(e),
                           "details": traceback.format_exc()})
        finally:
            self.logger.info(status)
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

    def get_object(self, obj_name):
        """
        Get TNS metadata for a given object name.
        :param obj_name: TNS object name, e.g., 2025xxx; str
        :return: metadata dictionary and empty dataframe (since we are not pulling lightcurve)
        """
        # Set meta and lc_df empty to start
        meta , lc_df = None, pd.DataFrame()
        # Initial status
        status = {"obj_name": obj_name}
        try:
            # Wait to avoid rate limiting
            time.sleep(self.config["tns"]["rate_limit"])
            # Run request to TNS server
            get_url = self.site + "/api/get/object"
            headers = {"User-Agent": self.marker}
            obj_request = OrderedDict([
                ("objid", ""),
                ("objname", obj_name),
                ("photometry", "0"),
                ("spectra", "0"),
            ])
            get_data = {"api_key": self.api_key, "data": json.dumps(obj_request)}
            response = requests.post(get_url, headers=headers, data=get_data)
            response_json = json.loads(response.text)
            # Meta
            if "data" not in response_json.keys():
                raise SurveyMetaMissing

            # Reduce meta to what we want
            status["status"] = "query success"
            result = response_json["data"]
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

        except SurveyMetaMissing:
            status['status'] = "failed to get TNS metadata"

        except Exception as e:
            status.update({"status": "encontered unexpected error",
                           "message": str(e),
                           "details": traceback.format_exc()})
        finally:
            self.logger.info(status)
            return meta, lc_df

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
