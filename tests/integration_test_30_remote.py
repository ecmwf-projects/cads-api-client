from typing import Any

import py
import pytest

from cads_api_client import catalogue, processing


def test_from_collection_to_process(api_root_url: str) -> None:
    collection_id = "dummy-dataset"
    cat = catalogue.Catalogue(f"{api_root_url}/catalogue")
    dataset = cat.collection(collection_id)

    res = dataset.retrieve_process()

    assert isinstance(res, processing.Process)


def test_collection_submit(api_root_url: str, api_key: str, request_year: str) -> None:
    collection_id = "dummy-dataset"
    headers = {"PRIVATE-TOKEN": api_key}

    cat = catalogue.Catalogue(f"{api_root_url}/catalogue", headers=headers)
    dataset = cat.collection(collection_id)

    res = dataset.submit()

    assert isinstance(res, processing.Remote)

    assert isinstance(res.request_uid, str)
    assert isinstance(res.status, str)


def test_collection_retrieve_with_dummy_adaptor(
    api_root_url: str, api_key: str, request_year: str, tmpdir: py.path.local
) -> None:
    collection_id = "dummy-dataset"
    headers = {"PRIVATE-TOKEN": api_key}

    cat = catalogue.Catalogue(f"{api_root_url}/catalogue", headers=headers)
    dataset = cat.collection(collection_id)
    target = str(tmpdir.join("dummy.txt"))

    res = dataset.retrieve(
        target=target,
        retry_options={"maximum_tries": 0},
    )

    assert isinstance(res, str)
    assert res.endswith(target)


def test_collection_retrieve_with_cds_adaptor(
    api_root_url: str, api_key: str, request_year: str, tmpdir: py.path.local
) -> None:
    collection_id = "reanalysis-era5-pressure-levels"
    headers = {"PRIVATE-TOKEN": api_key}
    accepted_licences: list[dict[str, Any]] = [
        {"id": "licence-to-use-copernicus-products", "revision": 12}
    ]

    cat = catalogue.Catalogue(f"{api_root_url}/catalogue", headers=headers)
    dataset = cat.collection(collection_id)
    target = str(tmpdir.join("era5.grib"))

    res = dataset.retrieve(
        accepted_licences=accepted_licences,
        product_type="reanalysis",
        variable="temperature",
        year=request_year,
        month="01",
        day="02",
        time="00:00",
        level="1000",
        target=target,
        retry_options={"maximum_tries": 0},
    )

    assert isinstance(res, str)
    assert res.endswith(target)


def test_collection_retrieve_with_ads_adaptor(
    api_root_url: str, api_key: str, request_year: str, tmpdir: py.path.local
) -> None:
    collection_id = "cams-global-reanalysis-eac4-monthly"
    headers = {"PRIVATE-TOKEN": api_key}
    accepted_licences: list[dict[str, Any]] = [
        {"id": "licence-to-use-copernicus-products", "revision": 12}
    ]

    cat = catalogue.Catalogue(f"{api_root_url}/catalogue", headers=headers)
    dataset = cat.collection(collection_id)
    target = str(tmpdir.join("eac4.grib"))

    res = dataset.retrieve(
        accepted_licences=accepted_licences,
        variable="particulate_matter_10um",
        year=request_year,
        month="02",
        product_type="monthly_mean",
        target=target,
        retry_options={"maximum_tries": 0},
    )

    assert isinstance(res, str)
    assert res.endswith(target)


def test_collection_retrieve_with_url_adaptor(
    api_root_url: str, api_key: str, request_year: str, tmpdir: py.path.local
) -> None:
    collection_id = "derived-near-surface-meteorological-variables"
    headers = {"PRIVATE-TOKEN": api_key}
    accepted_licences: list[dict[str, Any]] = [
        {"id": "licence-to-use-copernicus-products", "revision": 12}
    ]

    cat = catalogue.Catalogue(f"{api_root_url}/catalogue", headers=headers)
    dataset = cat.collection(collection_id)
    target = str(tmpdir.join("wfde1.zip"))

    res = dataset.retrieve(
        accepted_licences=accepted_licences,
        variable="grid_point_altitude",
        reference_dataset="cru",
        version="2.1",
        target=target,
        retry_options={"maximum_tries": 0},
    )

    assert isinstance(res, str)
    assert res.endswith(target)

    target = str(tmpdir.join("wfde2.zip"))

    res = dataset.retrieve(
        accepted_licences=accepted_licences,
        variable="grid_point_altitude",
        reference_dataset="cru",
        version="2.1",
        format="zip",
        target=target,
    )

    assert isinstance(res, str)
    assert res.endswith(target)


@pytest.mark.xfail  # type: ignore
def test_collection_retrieve_with_direct_mars_adaptor(
    api_root_url: str, api_key: str, request_year: str, tmpdir: py.path.local
) -> None:
    collection_id = "reanalysis-era5-complete"
    headers = {"PRIVATE-TOKEN": api_key}
    accepted_licences: list[dict[str, Any]] = [
        {"id": "licence-to-use-copernicus-products", "revision": 12}
    ]

    cat = catalogue.Catalogue(f"{api_root_url}/catalogue", headers=headers)
    dataset = cat.collection(collection_id)
    target = str(tmpdir.join("era5-complete.grib"))

    res = dataset.retrieve(
        accepted_licences=accepted_licences,
        date="20180101",
        levelist="1",
        levtype="ml",
        param="130",
        step="1",
        stream="mnth",
        time="06:00:00",
        type="fc",
        target=target,
        retry_options={"maximum_tries": 0},
        **{"class": "ea"},
    )

    assert isinstance(res, str)
    assert res.endswith(target)
