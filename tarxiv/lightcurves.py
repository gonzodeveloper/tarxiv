"""Pull and process lightcurves"""

import io
import requests
import numpy as np
import logging
import pandas as pd

from tarxiv.constants import FINKAPIURL, ATLASAPIURL
from tarxiv.constants import atlas_headers

_LOG = logging.getLogger(__name__)


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
        "i:magpsf": "MAG",
        "i:sigmapsf": "MAGERR",
        "i:fid": "FILTER",
        "i:jd": "TIME",
        "whatelse?": "TBD",
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



def mapping_atlas_to_tarxiv(atlas_json_data, include_nondets=False):
    """
    This function takes the ATLAS detections from the ATLAS JSON and turns them to OSC format

    Returns
    -------
    List of Dictionaries to be placed under the "photometry" key in our JSON

    """

    # DETECTIONS
    phot = pd.DataFrame(atlas_json_data['lc'])[['mjd', 'mag', 'magerr', 'filter', 'expname', ]]
    phot.columns = ['time', 'magnitude', 'e_magnitude', 'band', 'telescope']
    phot['upperlimit'] = False

    # NONDETECTIONS
    if include_nondets:
        photnon = pd.DataFrame(atlas_json_data['lcnondets'])[['mjd', 'mag5sig', 'input', 'filter', 'expname', ]]
        photnon.columns = ['time', 'magnitude', 'e_magnitude', 'band', 'telescope']
        photnon['e_magnitude'] = np.nan
        photnon['upperlimit'] = True

        # sort by MJD
        phot = pd.concat((phot, photnon)).sort_values('time', ascending=True)

    # adds a column to record which ATLAS unit the value was taken from
    phot['telescope'] = phot.telescope.apply(lambda x: x[:3]).values

    # adds a column for the survey name
    phot['survey'] = 'ATLAS'

    # shenanigans to return the list of dictionaries expected for photometry
    return list(phot.T.to_dict().values())


def get_atlas_lc(atlas_name=None, tns_name=None, coord=None):
    """Get data from ZTF based on either name or coordinates

    Parameters
    ----------
    atlas_name: str
        Internal ATLAS id (19 digits long, must end of a 0)
    tns_name: str
        Name of an object in TNS
    coord: tuple of float
        Object coordinates (RA, Dec), in degrees

    Returns
    -------
    List of Dictionaries

    """
    photometry = None

    if atlas_name is not None:
        # pdf = get_ztf_lc_from_ztf_name(ztf_name)
        raise Exception("Not implemented yet")
    elif tns_name is not None:
        photometry= get_atlas_lc_from_tns_name(tns_name)
    elif coord is not None and isinstance(coord, tuple):
        photometry = get_atlas_lc_from_coord(coord[0], coord[1])

    else:
        _LOG.error(
            "You should choose an object name or provide coordinates to get ATLAS lightcurves"
        )

    return photometry


def get_atlas_lc_from_atlas_id(
    atlas_id: str,
    mjd_threshold=60_000,  # should really set this dynamically
):
    """Get ATLAS data from ATLAS internal ID

    Parameters
    ----------
    atlas_id: str
        Internal ATLAS ID (19 digits long, must end of a 0)

    Returns
    -------
    List of Dictionaries to be placed under the "photometry" key in our JSON

    Examples
    --------
    >>> phot = get_atlas_lc_from_atlas_id("1022810791281932600")
    >>> assert not len(phot) == 0, "Oooops there should be data for ATLAS ID 1022810791281932600"
    """
    r = requests.post(
        f"{ATLASAPIURL}objects/",
        json={"objects": atlas_id, "mjd": mjd_threshold},
        headers=atlas_headers,
    )


    if r.status_code != 200:
        _LOG.warning(
            "Unable to get data for {} in ATLAS. HTTP error code: {}".format(
                atlas_id, r.status_code
            )
        )
        return pd.DataFrame()

    if r.json() == []:
        _LOG.warning("Data for the ATLAS ID {} not found".format(atlas_id))


    return mapping_atlas_to_tarxiv(r.json()[0], include_nondets = False)


def get_atlas_lc_from_tns_name(tns_name: str):
    """Get ATLAS data using a TNS object name

    Parameters
    ----------
    tns_name: str
        Name of a TNS object, e.g. SN 2024utu

    Returns
    -------
    pd.DataFrame
        Pandas DataFrame. Each row is a measurement.

    Examples
    --------
    >>> phot = get_atlas_lc_from_tns_name("2024utu")
    >>> assert not len(phot) == 0, "Oooops there should be data for SN 2024utu in ATLAS"
    """
    # TODO: strip names of initial AT or SN before putting into the payload
    # 1) Get the TNS name from ATLAS
    r = requests.post(
        f"{ATLASAPIURL}externalxmlist/",
        json={"externalObjects": tns_name},
        headers=atlas_headers,
    )
    if r.status_code != 200:
        _LOG.warning(
            "Unable to get data for {} in ATLAS. HTTP error code: {}".format(
                tns_name, r.status_code
            )
        )
        return pd.DataFrame()

    if r.json() == []:
        _LOG.warning("Data for the TNS name {} not found".format(tns_name))

    atlas_id = r.json()[0][0]["transient_object_id"]

    # 2) Get the ATLAS data from the ATLAS ID
    return get_atlas_lc_from_atlas_id(atlas_id)


def get_atlas_lc_from_coord(ra: float, dec: float, radius: float = 5.0):
    """Get ATLAS data from Cone Search. RA/Dec in degrees

    Note
    -----
    without a date threshold this is very very very slow.

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
    List of Dictionaries

    Examples
    --------
    >>> phot = get_atlas_lc_from_coord(37.044652, 28.326629)
    >>> assert not len(phot) == 0, "Oooops there should be data for SN 2024utu (ZTF24abeiqfc) in Fink!"

    # TODO: what's this below?
    # artifically getting blending
    # >>> out = get_atlas_lc_from_coord(37.044652, 28.326629, 60)


    # Check that crazy input returns empty output
    >>> out = get_atlas_lc_from_coord("h:m:s", "d:m:s")
    >>> assert out.empty, "Hum, you need coordinates in degree!"
    """
    # get matches in a conesearch

    r = requests.post(
        url=f"{ATLASAPIURL}cone/",
        json={"ra": ra, "dec": dec, "radius": radius, "requestType": "nearest"},
        headers=atlas_headers,
    )

    # check status
    if r.status_code != 200:
        _LOG.warning(
            "Unable to get data for ({}, {}, {}) in ATLAS. HTTP error code: {}".format(
                ra, dec, radius, r.status_code
            )
        )
        return pd.DataFrame()

    atlas_id = r.json()["object"]

    # get full lightcurves for all these alerts
    return get_atlas_lc_from_atlas_id(atlas_id)


if __name__ == "__main__":
    """Execute the test suite"""
    import sys
    import doctest

    sys.exit(doctest.testmod()[0])
