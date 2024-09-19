"""Pull and process lightcurves"""

import io
import requests
import logging

import pandas as pd

_LOG = logging.getLogger(__name__)
FINKAPIURL = "https://fink-portal.org"


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
        "https://fink-portal.org/api/v1/resolver",
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


def get_ztf_lc_from_coord(ra: float, dec: float):
    pass


if __name__ == "__main__":
    """Execute the test suite"""
    import sys
    import doctest

    sys.exit(doctest.testmod()[0])
