import os
import py
import pytest

from cads_api_client import catalogue

DATA_FORMAT_EXTENTIONS = {
    "grib": ".grib",
    "netcdf": ".nc",
}

@pytest.mark.parametrize("data_format", ("grib", "netcdf"))
def test_real_collection(
    api_root_url: str, api_key: str, tmpdir: py.path.local,
    data_format: str
) -> None:
    os.chdir(tmpdir)

    headers = {"PRIVATE-TOKEN": api_key}
    cat = catalogue.Catalogue(f"{api_root_url}/catalogue", headers=headers)


    collection_id = "reanalysis-era5-single-levels"
    dataset = cat.collection(collection_id)
    request = {
        "variable": "2m_temperature",
        "year": "2020",
        "month": "01",
        "day": "01",
        "time": "00:00",
        "data_format": data_format,
    }
    res = dataset.retrieve(
        **request
    )

    assert isinstance(res, str)
    assert res.endswith(DATA_FORMAT_EXTENTIONS[data_format])

