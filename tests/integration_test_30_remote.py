import datetime
import filecmp
import os
import pathlib

import pytest

from cads_api_client import ApiClient, catalogue, processing


@pytest.fixture
def cat(api_root_url: str, api_anon_key: str) -> catalogue.Catalogue:
    client = ApiClient(url=api_root_url, key=api_anon_key, maximum_tries=0)
    return client.catalogue_api


def test_from_collection_to_process(cat: catalogue.Catalogue) -> None:
    collection_id = "test-adaptor-dummy"
    dataset = cat.collection(collection_id)

    res = dataset.retrieve_process()

    assert isinstance(res, processing.Process)


def test_collection_submit(cat: catalogue.Catalogue) -> None:
    collection_id = "test-adaptor-dummy"
    dataset = cat.collection(collection_id)

    res = dataset.submit()

    assert isinstance(res, processing.Remote)

    assert isinstance(res.request_uid, str)
    assert isinstance(res.status, str)


def test_collection_retrieve_with_dummy_adaptor(
    cat: catalogue.Catalogue, tmp_path: pathlib.Path
) -> None:
    collection_id = "test-adaptor-dummy"
    dataset = cat.collection(collection_id)
    target = str(tmp_path / "dummy.txt")

    res = dataset.retrieve(
        _timestamp=datetime.datetime.now().isoformat(),
        target=target,
    )
    assert res == target
    assert os.path.exists(target)


def test_collection_retrieve_with_url_cds_adaptor(
    cat: catalogue.Catalogue, tmp_path: pathlib.Path
) -> None:
    collection_id = "test-adaptor-url"
    dataset = cat.collection(collection_id)
    request = {
        "variable": "grid_point_altitude",
        "reference_dataset": "cru",
        "version": "2.1",
        "format": "zip",
        "_timestamp": datetime.datetime.now().isoformat(),
    }
    target1 = str(tmp_path / "wfde1.zip")
    res = dataset.retrieve(
        **request,
        target=target1,
    )
    assert res == target1
    assert os.path.exists(target1)

    target2 = str(tmp_path / "wfde2.zip")
    res = dataset.retrieve(
        **request,
        target=target2,
    )
    assert filecmp.cmp(target1, target2)


def test_collection_retrieve_with_direct_mars_cds_adaptor(
    cat: catalogue.Catalogue, tmp_path: pathlib.Path
) -> None:
    collection_id = "test-adaptor-direct-mars"
    dataset = cat.collection(collection_id)
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
    res = dataset.retrieve(
        target=target,
        _timestamp=datetime.datetime.now().isoformat(),
        **request,
    )
    assert res == target
    assert os.path.exists(target)


def test_collection_retrieve_with_mars_cds_adaptor(
    cat: catalogue.Catalogue, tmp_path: pathlib.Path
) -> None:
    collection_id = "test-adaptor-mars"
    dataset = cat.collection(collection_id)
    request = {
        "product_type": "reanalysis",
        "variable": "2m_temperature",
        "year": "2016",
        "month": "01",
        "day": "02",
        "time": "00:00",
    }
    target = str(tmp_path / "era5.grib")
    res = dataset.retrieve(
        **request,
        _timestamp=datetime.datetime.now().isoformat(),
        target=target,
    )
    assert res == target
    assert os.path.exists(target)
