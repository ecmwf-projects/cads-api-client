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

    assert len(res.process_ids()) == 10


def test_processes_limit(api_root_url: str) -> None:
    proc = processing.Processing(f"{api_root_url}/retrieve")
    processes = proc.processes(params={"limit": 1})

    res = processes.next()

    if res is not None:
        assert res.response.status_code == 200


def test_process(api_root_url: str) -> None:
    process_id = "test-adaptor-dummy"
    proc = processing.Processing(f"{api_root_url}/retrieve")

    res = proc.process(process_id)

    assert isinstance(res, processing.Process)
    assert res.id == process_id
    assert "links" in res.json
    assert isinstance(res.json["links"], list)


def test_validate_constraints(api_root_url: str) -> None:
    process_id = "test-adaptor-mars"
    proc = processing.Processing(f"{api_root_url}/retrieve")
    process = proc.process(process_id)
    res = process.valid_values({})

    assert set(["product_type", "variable", "year", "month", "time"]) <= set(res)


def test_collection_missing_licence(
    api_root_url: str, api_key_anon: str, request_year: str
) -> None:
    collection_id = "test-adaptor-mars"
    headers = {"PRIVATE-TOKEN": api_key_anon}
    proc = processing.Processing(f"{api_root_url}/retrieve", headers=headers)
    process = proc.process(collection_id)

    with pytest.raises(RuntimeError, match="403 Client Error"):
        _ = process.execute(
            inputs=dict(
                product_type="reanalysis",
                variable="temperature",
                year=request_year,
                month="01",
                day="01",
                time="00:00",
                level="1000",
            ),
        )


def test_jobs_list(api_root_url: str, api_key: str, request_year: str) -> None:
    collection_id = "test-adaptor-dummy"
    headers = {"PRIVATE-TOKEN": api_key}
    proc = processing.Processing(f"{api_root_url}/retrieve", headers=headers)
    process = proc.process(collection_id)

    _ = process.execute(inputs={})
    _ = process.execute(inputs={})

    res = proc.jobs().json
    assert len(res["jobs"]) >= 2

    res = proc.jobs(params={"limit": 1}).json
    assert len(res["jobs"]) == 1

    jobs = proc.jobs(params={"limit": 1})
    res = jobs.next().json  # type: ignore

    assert res is not None
    assert len(res["jobs"]) == 1


def test_validate_constraints_error(api_root_url: str) -> None:
    process_id = "test-adaptor-mars"
    proc = processing.Processing(f"{api_root_url}/retrieve")
    process = proc.process(process_id)
    with pytest.raises(RuntimeError, match="422 Client Error"):
        process.valid_values({"invalid_param": 1})
