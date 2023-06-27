from typing import Any

import py
import pytest
import requests

from cads_api_client import catalogue, processes, jobs, APIClient


# TODO move to catalogue
def test_from_collection_to_process(api_root_url: str) -> None:
    collection_id = "test-adaptor-dummy"
    collection = catalogue.Collection(collection_id, base_url=api_root_url)
    proc = collection.retrieve_process()
    assert isinstance(proc, processes.Process)


# TODO split
def test_collection_submit(api_root_url: str, api_key: str, request_year: str) -> None:
    collection_id = "test-adaptor-dummy"
    # headers = {"PRIVATE-TOKEN": api_key}
    # cat = catalogue.Catalogue(f"{api_root_url}/catalogue", headers=headers)
    # dataset = cat.collection(collection_id)
    collection = catalogue.Collection(collection_id, base_url=api_root_url, api_key=api_key)

    # res = dataset.submit()
    res = collection.retrieve_process().execute({},[])

    assert isinstance(res, jobs.Job)

    assert isinstance(res.id, str)
    assert isinstance(res.status, str)


def test_collection_retrieve_with_dummy_adaptor(
    api_root_url: str, api_key: str, request_year: str, tmpdir: py.path.local
) -> None:
    collection_id = "test-adaptor-dummy"
    # headers = {"PRIVATE-TOKEN": api_key}
    # cat = catalogue.Catalogue(f"{api_root_url}/catalogue", headers=headers)
    # dataset = cat.collection(collection_id)
    client = APIClient(base_url=api_root_url, api_key=api_key)
    target = str(tmpdir.join("dummy.txt"))

    res = client.retrieve(
        collection_id=collection_id,
        inputs=[{}],
        target=target,
        # retry_options={"maximum_tries": 0},
    )

    assert isinstance(res, str)
    assert res.endswith(target)


# TODO
# def test_multi_retrieve():
#     pass


def test_collection_retrieve_with_url_cds_adaptor(
    api_root_url: str, api_key: str, request_year: str, tmpdir: py.path.local
) -> None:
    collection_id = "test-adaptor-url"
    # headers = {"PRIVATE-TOKEN": api_key}
    accepted_licences: list[dict[str, Any]] = [
        {"id": "licence-to-use-copernicus-products", "revision": 12}
    ]

    # cat = catalogue.Catalogue(f"{api_root_url}/catalogue", headers=headers)
    # dataset = cat.collection(collection_id)
    target = str(tmpdir.join("wfde1.zip"))

    client = APIClient(base_url=api_root_url, api_key=api_key)
    inputs = dict(
        variable="grid_point_altitude",
        reference_dataset="cru",
        version="2.1"
    )

    res = client.retrieve(
        collection_id=collection_id,
        inputs=[inputs],
        accepted_licences=accepted_licences,
        target=target,
        # retry_options={"maximum_tries": 0},
    )

    assert isinstance(res, str)
    assert res.endswith(target)

    target = str(tmpdir.join("wfde2.zip"))

    inputs = dict(
        variable="grid_point_altitude",
        reference_dataset="cru",
        version="2.1",
        format="zip",
    )

    res = client.retrieve(
        collection_id=collection_id,
        inputs=[inputs],
        accepted_licences=accepted_licences,
        target=target,
    )

    assert isinstance(res, str)
    assert res.endswith(target)


def test_collection_retrieve_with_direct_mars_cds_adaptor(
    api_root_url: str, api_key: str, request_year: str, tmpdir: py.path.local
) -> None:
    collection_id = "test-adaptor-direct-mars"
    # headers = {"PRIVATE-TOKEN": api_key}
    accepted_licences: list[dict[str, Any]] = [
        {"id": "licence-to-use-copernicus-products", "revision": 12}
    ]

    # cat = catalogue.Catalogue(f"{api_root_url}/catalogue", headers=headers)
    # dataset = cat.collection(collection_id)
    target = str(tmpdir.join("era5-complete.grib"))
    client = APIClient(base_url=api_root_url, api_key=api_key)
    inputs = dict(
        levelist=1,
        dataset="reanalysis",
        time="00:00:00",
        param="155",
        date="1940-01-01",
        expect="any",
        levtype="pl",
        number="all",
    )

    res = client.retrieve(
        accepted_licences=accepted_licences,
        inputs=[inputs],
        target=target,
        # retry_options={"maximum_tries": 0},
        **{"class": "ea"},
    )

    assert isinstance(res, str)
    assert res.endswith(target)


def test_collection_retrieve_with_mars_cds_adaptor(
    api_root_url: str, api_key: str, request_year: str, tmpdir: py.path.local
) -> None:
    collection_id = "test-adaptor-mars"
    # headers = {"PRIVATE-TOKEN": api_key}
    accepted_licences: list[dict[str, Any]] = [
        {"id": "licence-to-use-copernicus-products", "revision": 12}
    ]

    # cat = catalogue.Catalogue(f"{api_root_url}/catalogue", headers=headers)
    # dataset = cat.collection(collection_id)
    client = APIClient(base_url=api_root_url, api_key=api_key)
    target = str(tmpdir.join("era5.grib"))

    inputs = dict(
        product_type="reanalysis",
        variable="2m_temperature",
        year=request_year,
        month="01",
        day="02",
        time="00:00",
    )

    res = client.retrieve(
        collection_id=collection_id,
        accepted_licences=accepted_licences,
        inputs=[inputs],
        target=target,
        # retry_options={"maximum_tries": 0},
    )

    assert isinstance(res, str)
    assert res.endswith(target)


def test_collection_retrieve_with_legacy_cds_adaptor(
    api_root_url: str, api_key: str, request_year: str, tmpdir: py.path.local
) -> None:
    collection_id = "test-adaptor-legacy"
    # headers = {"PRIVATE-TOKEN": api_key}
    accepted_licences: list[dict[str, Any]] = [
        {"id": "licence-to-use-copernicus-products", "revision": 12}
    ]

    # cat = catalogue.Catalogue(f"{api_root_url}/catalogue", headers=headers)
    # dataset = cat.collection(collection_id)
    client = APIClient(base_url=api_root_url, api_key=api_key)
    target = str(tmpdir.join("era5.grib"))

    inputs = dict(
        product_type="reanalysis",
        variable="temperature",
        year=request_year,
        month="01",
        day="02",
        time="00:00",
        level="1000",
    )

    res = client.retrieve(
        collection_id=collection_id,
        accepted_licences=accepted_licences,
        inputs=[inputs],
        target=target,
        # retry_options={"maximum_tries": 0},
    )

    assert isinstance(res, str)
    assert res.endswith(target)
