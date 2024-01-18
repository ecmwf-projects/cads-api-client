import py
import pytest

from cads_api_client import catalogue, processing


def test_from_collection_to_process(api_root_url: str) -> None:
    collection_id = "test-adaptor-dummy"
    cat = catalogue.Catalogue(f"{api_root_url}/catalogue")
    dataset = cat.collection(collection_id)

    res = dataset.retrieve_process()

    assert isinstance(res, processing.Process)


def test_collection_submit(api_root_url: str, api_key: str, request_year: str) -> None:
    collection_id = "test-adaptor-dummy"
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
    collection_id = "test-adaptor-dummy"
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


def test_collection_retrieve_with_url_cds_adaptor(
    api_root_url: str, api_key: str, request_year: str, tmpdir: py.path.local
) -> None:
    collection_id = "test-adaptor-url"
    headers = {"PRIVATE-TOKEN": api_key}

    cat = catalogue.Catalogue(f"{api_root_url}/catalogue", headers=headers)
    dataset = cat.collection(collection_id)
    target = str(tmpdir.join("wfde1.zip"))

    res = dataset.retrieve(
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
        variable="grid_point_altitude",
        reference_dataset="cru",
        version="2.1",
        format="zip",
        target=target,
    )

    assert isinstance(res, str)
    assert res.endswith(target)


def test_collection_retrieve_with_direct_mars_cds_adaptor(
    api_root_url: str, api_key: str, request_year: str, tmpdir: py.path.local
) -> None:
    collection_id = "test-adaptor-direct-mars"
    headers = {"PRIVATE-TOKEN": api_key}

    cat = catalogue.Catalogue(f"{api_root_url}/catalogue", headers=headers)
    dataset = cat.collection(collection_id)
    target = str(tmpdir.join("era5-complete.grib"))

    res = dataset.retrieve(
        levelist=1,
        dataset="reanalysis",
        time="00:00:00",
        param="155",
        date="1940-01-01",
        expect="any",
        levtype="pl",
        number="all",
        target=target,
        retry_options={"maximum_tries": 0},
        **{"class": "ea"},
    )

    assert isinstance(res, str)
    assert res.endswith(target)


def test_collection_retrieve_with_mars_cds_adaptor(
    api_root_url: str, api_key: str, request_year: str, tmpdir: py.path.local
) -> None:
    collection_id = "test-adaptor-mars"
    headers = {"PRIVATE-TOKEN": api_key}

    cat = catalogue.Catalogue(f"{api_root_url}/catalogue", headers=headers)
    dataset = cat.collection(collection_id)
    target = str(tmpdir.join("era5.grib"))

    res = dataset.retrieve(
        product_type="reanalysis",
        variable="2m_temperature",
        year=request_year,
        month="01",
        day="02",
        time="00:00",
        target=target,
        retry_options={"maximum_tries": 0},
    )

    assert isinstance(res, str)
    assert res.endswith(target)


@pytest.mark.skip(reason="discontinued adaptor")
def test_collection_retrieve_with_legacy_cds_adaptor(
    api_root_url: str, api_key: str, request_year: str, tmpdir: py.path.local
) -> None:
    collection_id = "test-adaptor-legacy"
    headers = {"PRIVATE-TOKEN": api_key}

    cat = catalogue.Catalogue(f"{api_root_url}/catalogue", headers=headers)
    dataset = cat.collection(collection_id)
    target = str(tmpdir.join("era5.grib"))

    res = dataset.retrieve(
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
