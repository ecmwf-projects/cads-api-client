import datetime
import filecmp
import os
import pathlib

from cads_api_client import ApiClient


def test_adaptors_dummy(api_anon_client: ApiClient, tmp_path: pathlib.Path) -> None:
    collection_id = "test-adaptor-dummy"
    target = str(tmp_path / "dummy.grib")
    remote = api_anon_client.submit(
        collection_id,
        _timestamp=datetime.datetime.now().isoformat(),
    )
    assert remote.download(target) == target
    assert os.path.exists(target)


def test_adaptors_url(api_anon_client: ApiClient, tmp_path: pathlib.Path) -> None:
    collection_id = "test-adaptor-url"
    request = {
        "variable": "grid_point_altitude",
        "reference_dataset": "cru",
        "version": "2.1",
        "format": "zip",
        "_timestamp": datetime.datetime.now().isoformat(),
    }
    target1 = str(tmp_path / "wfde1.zip")
    remote = api_anon_client.submit(collection_id, **request)
    assert remote.download(target1) == target1
    assert os.path.exists(target1)

    target2 = str(tmp_path / "wfde2.zip")
    remote = api_anon_client.submit(collection_id, **request)
    assert remote.download(target2) == target2
    assert filecmp.cmp(target1, target2)


def test_adaptors_direct_mars(
    api_anon_client: ApiClient, tmp_path: pathlib.Path
) -> None:
    collection_id = "test-adaptor-direct-mars"
    request = {
        "levelist": "1",
        "dataset": "reanalysis",
        "time": "00:00:00",
        "param": "155",
        "date": "1940-01-01",
        "expect": "any",
        "levtype": "pl",
        "number": "all",
        "class": "ea",
    }
    target = str(tmp_path / "era5-complete.grib")
    remote = api_anon_client.submit(
        collection_id,
        _timestamp=datetime.datetime.now().isoformat(),
        **request,
    )
    assert remote.download(target) == target
    assert os.path.exists(target)


def test_adaptors_mars(api_anon_client: ApiClient, tmp_path: pathlib.Path) -> None:
    collection_id = "test-adaptor-mars"
    request = {
        "product_type": "reanalysis",
        "variable": "2m_temperature",
        "year": "2016",
        "month": "01",
        "day": "02",
        "time": "00:00",
    }
    target = str(tmp_path / "era5.grib")
    remote = api_anon_client.submit(
        collection_id,
        **request,
        _timestamp=datetime.datetime.now().isoformat(),
    )
    assert remote.download(target) == target
    assert os.path.exists(target)
