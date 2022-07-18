from cads_api_client import catalogue, processing


def test_processes(dev_env_api_url: str) -> None:
    proc = processing.Processing(f"{dev_env_api_url}/processing")

    res = proc.processes()

    assert isinstance(res, dict)
    assert "processes" in res
    assert isinstance(res["processes"], list)
    assert "links" in res
    assert isinstance(res["links"], list)


def test_process_ids(dev_env_api_url: str) -> None:
    process_id = "retrieve-reanalysis-era5-land-monthly-means"
    proc = processing.Processing(f"{dev_env_api_url}/processing")

    res = proc.process_ids()

    assert isinstance(res, list)
    assert process_id in res


def test_process(dev_env_api_url: str) -> None:
    process_id = "retrieve-reanalysis-era5-land-monthly-means"
    proc = processing.Processing(f"{dev_env_api_url}/processing")

    res = proc.process(process_id)

    assert isinstance(res, dict)
    assert "id" in res
    assert res["id"] == process_id
    assert "links" in res
    assert isinstance(res["links"], list)


def test_from_collection_to_process(dev_env_api_url: str) -> None:
    collection_id = "reanalysis-era5-land-monthly-means"
    cat = catalogue.Catalogue(f"{dev_env_api_url}/catalogue")

    dataset = cat.collection(collection_id)
    links = dataset["links"]

    process_id = processing.get_process_id_from_links(links)

    proc = processing.Processing("http://localhost:8080/api/processing")

    res = proc.process(process_id)

    assert res
