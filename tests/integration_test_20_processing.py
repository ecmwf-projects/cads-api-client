import pytest

from cads_api_client import processing


def test_processes(api_root_url: str) -> None:
    proc = processing.Processing(f"{api_root_url}/retrieve")

    res = proc.processes()

    assert isinstance(res, processing.ProcessList)
    assert "processes" in res.json
    assert isinstance(res.json["processes"], list)
    assert "links" in res.json
    assert isinstance(res.json["links"], list)

    expected_process_id = "reanalysis-era5-land-monthly-means"

    assert expected_process_id in res.process_ids()


def test_process(api_root_url: str) -> None:
    process_id = "reanalysis-era5-land-monthly-means"
    proc = processing.Processing(f"{api_root_url}/retrieve")

    res = proc.process(process_id)

    assert isinstance(res, processing.Process)
    assert res.id == process_id
    assert "links" in res.json
    assert isinstance(res.json["links"], list)


def test_validate_constraints(api_root_url: str) -> None:
    process_id = "reanalysis-era5-land-monthly-means"
    proc = processing.Processing(f"{api_root_url}/retrieve")
    process = proc.process(process_id)
    res = process.valid_values({})

    assert set(["product_type", "variable", "year", "month", "time"]) <= set(res)


def test_validate_constraints_error(api_root_url: str) -> None:
    process_id = "reanalysis-era5-land-monthly-means"
    proc = processing.Processing(f"{api_root_url}/retrieve")
    process = proc.process(process_id)
    with pytest.raises(RuntimeError):
        process.valid_values({"invalid_param": 1})
