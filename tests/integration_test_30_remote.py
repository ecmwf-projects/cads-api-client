import py

from cads_api_client import catalogue, processing


def test_from_collection_to_process(dev_env_api_url: str) -> None:
    collection_id = "reanalysis-era5-land-monthly-means"
    cat = catalogue.Catalogue(f"{dev_env_api_url}/catalogue")
    dataset = cat.collection(collection_id)

    res = dataset.retrieve_process()

    assert isinstance(res, processing.Process)


def test_collection_submit(dev_env_api_url: str) -> None:
    collection_id = "reanalysis-era5-pressure-levels"
    cat = catalogue.Catalogue(f"{dev_env_api_url}/catalogue")
    dataset = cat.collection(collection_id)

    res = dataset.submit(
        product_type="reanalysis",
        variable="temperature",
        year="2022",
        month="01",
        day="01",
        time="00:00",
        level="1000",
    )

    assert isinstance(res, processing.Remote)

    assert isinstance(res.request_uid, str)
    assert isinstance(res.status, str)


def test_collection_retrieve_with_cds_adaptor(
    dev_env_api_url: str, tmpdir: py.path.local
) -> None:
    collection_id = "reanalysis-era5-pressure-levels"
    cat = catalogue.Catalogue(f"{dev_env_api_url}/catalogue")
    dataset = cat.collection(collection_id)
    target = str(tmpdir.join("era5.grib"))

    res = dataset.retrieve(
        product_type="reanalysis",
        variable="temperature",
        year="2022",
        month="01",
        day="02",
        time="00:00",
        level="1000",
        target=target,
        retry_options={"maximum_tries": 0},
    )

    assert isinstance(res, str)
    assert res.endswith(target)


def test_collection_retrieve_with_cams_adaptor(
    dev_env_api_url: str, tmpdir: py.path.local
) -> None:
    collection_id = "cams-global-reanalysis-eac4-monthly"
    cat = catalogue.Catalogue(f"{dev_env_api_url}/catalogue")
    dataset = cat.collection(collection_id)
    target = str(tmpdir.join("eac4.grib"))

    res = dataset.retrieve(
        variable="particulate_matter_10um",
        year="2016",
        month="02",
        product_type="monthly_mean",
        target=target,
        retry_options={"maximum_tries": 0},
    )

    assert isinstance(res, str)
    assert res.endswith(target)
