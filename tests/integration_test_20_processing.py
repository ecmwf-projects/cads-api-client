from cads_api_client import catalogue, processing


def test_processes(dev_env_api_url: str) -> None:
    proc = processing.Processing(f"{dev_env_api_url}/retrieve")

    res = proc.processes()

    assert isinstance(res, processing.ProcessList)
    assert "processes" in res.json
    assert isinstance(res.json["processes"], list)
    assert "links" in res.json
    assert isinstance(res.json["links"], list)

    expected_process_id = "reanalysis-era5-land-monthly-means"

    assert expected_process_id in res.process_ids()


def test_process(dev_env_api_url: str) -> None:
    process_id = "reanalysis-era5-land-monthly-means"
    proc = processing.Processing(f"{dev_env_api_url}/retrieve")

    res = proc.process(process_id)

    assert isinstance(res, processing.Process)
    assert "id" in res.json
    assert res.json["id"] == process_id
    assert "links" in res.json
    assert isinstance(res.json["links"], list)


def test_from_collection_to_process(dev_env_api_url: str) -> None:
    collection_id = "reanalysis-era5-land-monthly-means"
    cat = catalogue.Catalogue(f"{dev_env_api_url}/catalogue")

    dataset = cat.collection(collection_id)

    res = dataset.retrieve_process()

    assert isinstance(res, processing.Process)
