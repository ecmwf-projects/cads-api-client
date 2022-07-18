from cads_api_client import processing


def test_processes() -> None:
    proc = processing.Processing("http://localhost:8080/api/processing")

    res = proc.processes()

    assert isinstance(res, dict)
    assert "processes" in res
    assert isinstance(res["processes"], list)
    assert "links" in res
    assert isinstance(res["links"], list)


def test_collection_ids() -> None:
    process_id = "retrieve-reanalysis-era5-land-monthly-means"
    proc = processing.Processing("http://localhost:8080/api/processing")

    res = proc.process_ids()

    assert isinstance(res, list)
    assert process_id in res


def test_collection() -> None:
    process_id = "retrieve-reanalysis-era5-land-monthly-means"
    proc = processing.Processing("http://localhost:8080/api/processing")

    res = proc.process(process_id)

    assert isinstance(res, dict)
    assert "id" in res
    assert res["id"] == process_id
    assert "links" in res
    assert isinstance(res["links"], list)
