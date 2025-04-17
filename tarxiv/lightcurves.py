"""Pull and process lightcurves"""

from pyasassn.client import SkyPatrolClient
from astropy.time import Time
import atlasapiclient.client as atlas_client
import io
import re
import requests
import yaml
import logging
import pandas as pd




class LightCurves:

    def __init__(self, config_file):
        # Read in config
        self.config_file = config_file
        with open(config_file) as stream:
            self.config = yaml.safe_load(stream)

        self.logger = logging.getLogger("tarxiv_lightcurves")

    def get_light_all_light_curves(self):
        pass


    def get_asas_sn_lc(self, ra_deg, dec_deg, radius=15):
        """
        Get ASAS-SN Lightcurve curve from coordinates using cone_search.
        """
        client = SkyPatrolClient(verbose=False)
        query = f"WITH sources AS                " \ 
                f"  (                            " \
                f"      SELECT                   " \
                f"          asas_sn_id,          " \
                f"          ra_deg,              " \
                f"          dec_deg,             " \
                f"          catalog_sources,     " \
                f"          DISTANCE(ra_deg, dec_deg, {ra_deg}, {dec_deg}) * 3600 AS angular_dist " \
                f"     FROM master_list          " \ 
                f"  )                            " \
                f"SELECT *                       " \
                f"FROM sources                   " \
                f"WHERE angular_dist <= {radius} " \
                f"ORDER BY angular_dist ASC      "

        query = re.sub(r'(\s+)', ' ', query)
        lcs = client.adql_query(query, download=True)

        # Get nearest
        nearest = lcs.catalog_info.iloc[0]
        nearest_id = nearest['asas_sn_id']
        nearest_dist = nearest['angular_dist']
        catalog_sources = {catalog: client.query_list(int(nearest_id), catalog=catalog, id_col='asas_sn_id')['name'].to_list()[0]
                           for catalog in nearest['catalog_sources']}
        meta = {'asas_sn_id': nearest_id, 'cross_match_distance': nearest_dist, 'catalog_sources': catalog_sources}
        lc_df =  lcs[nearest_id].data
        lc_df['mjd'] = lc_df.apply(lambda row: Time(row['jd'], format='jd').mjd, axis=1)
        lc_df.rename({"phot_filter": "filter"}, axis=1, inplace=True)

        # Only return detections from not bad images
        lc_df = lc_df[(lc_df['mag_err'] < 99) & (lc_df['quality'] != "B")]
        return meta, lc_df[['mjd', 'mag', 'mag_err', 'filter']]

    def get_ztf_lc(self, ra_deg, dec_deg, radius=15):
        result = requests.post(
            f"{self.config['fink_url']}/api/v1/conesearch",
            json={"ra": ra_deg, "dec": dec_deg, "radius": radius, "columns": "i:objectId"},
        )
        # check status
        if result.status_code != 200:
            self.logger.warning({"status": "fink_error", "http_code": result.status_code})
            return None

        # get data for the match
        matches = [val["i:objectId"] for val in r.json()]
        if len(matches) == 0:
            self.logger.info({"status": "fink_cone_search_miss"})
            return None
        # Lightcurve columns and values
        cols = {
            "i:magpsf": "mag",
            "i:sigmapsf": "mag_err",
            "i:fid": "filter",
            "i:jd": "jd",
            }
        filter_map = {'1': 'g', '2': 'R', '3': 'i'}
        # Query
        result = requests.post(
            f"{self.config['fink_url']}/api/v1/objects",
            json={"objectId": matches[0], "columns": cols.keys()}
        )
        # check status
        if result.status_code != 200:
            self.logger.warning({"status": "fink_error", "http_code": result.status_code})
            return None
        # check returns
        if result.json() == []:
            self.logger.info({"status": "fink_ztf_id_miss"})
            return None
        lc_df = pd.read_json(io.BytesIO(result.content))
        lc_df = lc_df.rename(cols, axis=1)
        lc_df['mjd'] = lc_df.apply(lambda row: Time(row['jd'], format='jd').mjd, axis=1)
        lc_df["filter"] = lc_df['filter'].astype(str).map(filter_map)
        return lc_df


    def get_atlas_lc(self, ra_deg, dec_deg, radius=15):
        # First run cone search to get id
        cone = atlas_client.ConeSearch(api_config_file=self.config_file,
                                 payload={"ra": ra_deg,
                                          "dec": dec_deg,
                                          "radius": radius,
                                          "requestType": "nearest"},
                                 get_response=True)
        atlas_id = cone.response_data['object_id'] #???
        # Get light curve
        curve = atlas_client.RequestSingleSourceData(api_config_file=self.config_file,
                                                     atlas_id=atlas_id,
                                                     get_response=True)
        meta = curve.response_data[0]['object']
        lc_json = curve.response_data[0]['lc']

        # Process in dataframe
        lc_df = pd.DataFrame(lc_json)
        return lc_df

def mapping_ztf_to_tarxiv():
    """Mapping between ZTF column names and tarxiv column names

    Returns
    -------
    dict
        Dictionary containing mapping between
        Fink column names and tarxiv column names
    """
    # TODO: define tarxiv column names
    dic = {
        "i:magpsf": "mag",
        "i:sigmapsf": "mag_err",
        "i:fid": "filter",
        "i:jd": "jd",
    }

    return dic


def get_ztf_lc(ztf_name=None, tns_name=None, coord=None):
    """Get data from ZTF based on either name or coordinates

    Parameters
    ----------
    ztf_name: str
        Name of a ZTF object, starting with `ZTF`
    tns_name: str
        Name of an object in TNS
    coord: tuple of float
        Object coordinates (RA, Dec), in degrees

    Returns
    -------
    pd.DataFrame
        Pandas DataFrame containing ZTF data from Fink for
        the matching object in TNS. Each row is a measurement.

    """
    if ztf_name is not None:
        pdf = get_ztf_lc_from_ztf_name(ztf_name)
    elif tns_name is not None:
        pdf = get_ztf_lc_from_tns_name(tns_name)
    elif coord is not None and isinstance(coord, tuple):
        pdf = get_ztf_lc_from_coord(coord[0], coord[1])
    else:
        _LOG.error(
            "You should choose an object name or provide coordinates to get ZTF lightcurves"
        )

    # TODO: perform the column name conversion before returning
    # TODO: `mapping_ztf_to_tarxiv`

    # TODO: do we want to return a DataFrame or JSON is fine?
    # TODO: JSON will be faster (no conversion), but messier.
    return pdf


def get_ztf_lc_from_ztf_name(ztf_name: str):
    """Get ZTF data from Fink using a ZTF objectId

    Parameters
    ----------
    ztf_name: str
        Name of a ZTF object, starting with `ZTF`

    Returns
    -------
    pd.DataFrame
        Pandas DataFrame containing ZTF data from Fink for
        the matching object in TNS. Each row is a measurement.

    Examples
    --------
    >>> pdf = get_ztf_lc_from_ztf_name("ZTF24abeiqfc")
    >>> assert not pdf.empty, "Oooops there should be data for ZTF24abeiqfc in Fink!"


    # Check that crazy input returns empty output
    >>> out = get_ztf_lc_from_ztf_name("toto")
    >>> assert out.empty, "Hum, there is no `toto` in Fink!"
    """
    # get the relevant columns to download
    cols = mapping_ztf_to_tarxiv().keys()

    r = requests.post(
        "{}/api/v1/objects".format(FINKAPIURL),
        json={"objectId": ztf_name, "columns": ",".join(cols), "output-format": "json"},
    )

    if r.status_code != 200:
        _LOG.warning(
            "Unable to get data for {} in Fink. HTTP error code: {}".format(
                ztf_name, r.status_code
            )
        )
        return pd.DataFrame()

    if r.json() == []:
        _LOG.warning("Data for the ZTF name {} not found on Fink".format(ztf_name))

    # TODO: do we really need the pandas conversion?
    pdf = pd.read_json(io.BytesIO(r.content))

    return pdf


def get_ztf_lc_from_tns_name(tns_name: str):
    """Get ZTF data from Fink using a TNS object name

    Parameters
    ----------
    tns_name: str
        Name of a TNS object, e.g. SN 2024utu

    Returns
    -------
    pd.DataFrame
        Pandas DataFrame containing ZTF data from Fink for
        the matching object in TNS. Each row is a measurement.

    Examples
    --------
    >>> pdf = get_ztf_lc_from_tns_name("SN 2024utu")
    >>> assert not pdf.empty, "Oooops there should be data for SN 2024utu (ZTF24abeiqfc) in Fink!"


    # Check that crazy input returns empty output
    >>> out = get_ztf_lc_from_tns_name("toto")
    >>> assert out.empty, "Hum, there is no `toto` in Fink!"
    """
    # first use the Fink resolver to get corresponding ZTF name
    r = requests.post(
        "{}/api/v1/resolver".format(FINKAPIURL),
        json={"resolver": "tns", "name": tns_name},
    )

    # check status
    if r.status_code != 200:
        _LOG.warning(
            "Unable to get data for {} in Fink. HTTP error code: {}".format(
                tns_name, r.status_code
            )
        )
        return pd.DataFrame()

    if r.json() != []:
        for section in r.json():
            # extract ZTF name
            if section["d:internalname"].startswith("ZTF"):
                ztf_name = section["d:internalname"]
                return get_ztf_lc_from_ztf_name(ztf_name)

    _LOG.warning(
        "We could not find ZTF counterpart in Fink for the TNS object name {}".format(
            tns_name
        )
    )
    return pd.DataFrame()


def get_ztf_lc_from_coord(ra: float, dec: float, radius: float = 5.0):
    """Get ZTF data from Fink using its coordinates RA/Dec in degrees

    Parameters
    ----------
    ra: float
        Right ascension in degree
    dec: float
        Declination in degree
    radius: float, optional
        Conesearch radius, in arcsecond. Default is 5.0

    Returns
    -------
    pd.DataFrame
        Pandas DataFrame containing ZTF data from Fink for
        the matching object in TNS. Each row is a measurement.

    Examples
    --------
    >>> pdf = get_ztf_lc_from_coord(37.044652, 28.326629)
    >>> assert not pdf.empty, "Oooops there should be data for SN 2024utu (ZTF24abeiqfc) in Fink!"

    # artifically getting blending
    >>> out = get_ztf_lc_from_coord(37.044652, 28.326629, 60)


    # Check that crazy input returns empty output
    >>> out = get_ztf_lc_from_coord("h:m:s", "d:m:s")
    >>> assert out.empty, "Hum, you need coordinates in degree!"
    """
    # get matches in a conesearch
    r = requests.post(
        "{}/api/v1/conesearch".format(FINKAPIURL),
        json={"ra": ra, "dec": dec, "radius": radius, "columns": "i:objectId"},
    )

    # check status
    if r.status_code != 200:
        _LOG.warning(
            "Unable to get data for ({}, {}, {}) in Fink. HTTP error code: {}".format(
                ra, dec, radius, r.status_code
            )
        )
        return pd.DataFrame()

    # get data for the match
    matches = [val["i:objectId"] for val in r.json()]

    if len(matches) == 0:
        _LOG.warning("0 match in the conesearch ({}, {}, {})".format(ra, dec, radius))
        return pd.DataFrame()

    if len(matches) > 1:
        _LOG.warning(
            "{} matches from the conesearch ({}, {}, {}) with object ID: {}. We will take the first ID. Maybe you want to reduce the conesearch radius".format(
                len(matches), ra, dec, radius, str(matches)
            )
        )

    # get full lightcurves for all these alerts
    return get_ztf_lc_from_ztf_name(matches[0])

if __name__ == "__main__":
    """Execute the test suite"""
    import sys
    import doctest

    sys.exit(doctest.testmod()[0])
