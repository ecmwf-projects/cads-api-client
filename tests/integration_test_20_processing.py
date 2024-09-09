import pytest
import requests

from cads_api_client import ApiClient, processing


@pytest.fixture
def proc(api_root_url: str, api_anon_key: str) -> processing.Processing:
    client = ApiClient(url=api_root_url, key=api_anon_key, maximum_tries=0)
    return client.retrieve_api


def test_processes(proc: processing.Processing) -> None:
    res = proc.processes()

    assert isinstance(res, processing.ProcessList)
    assert "processes" in res.json
    assert isinstance(res.json["processes"], list)
    assert "links" in res.json
    assert isinstance(res.json["links"], list)

    assert len(res.process_ids()) == 10


def test_processes_limit(proc: processing.Processing) -> None:
    processes = proc.processes(params={"limit": 1})

    res = processes.next()

    if res is not None:
        assert res.response.status_code == 200


def test_process(proc: processing.Processing) -> None:
    process_id = "test-adaptor-dummy"

    res = proc.process(process_id)

    assert isinstance(res, processing.Process)
    assert res.id == process_id
    assert "links" in res.json
    assert isinstance(res.json["links"], list)


def test_validate_constraints(proc: processing.Processing) -> None:
    process_id = "test-adaptor-mars"
    process = proc.process(process_id)
    res = process.valid_values({})

    assert set(["product_type", "variable", "year", "month", "time"]) <= set(res)


def test_collection_anonymous_user(proc: processing.Processing) -> None:
    collection_id = "test-adaptor-mars"
    process = proc.process(collection_id)
    response = process.execute(inputs={})
    assert "message" in response.json


def test_jobs_list(proc: processing.Processing) -> None:
    collection_id = "test-adaptor-dummy"
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


def test_validate_constraints_error(proc: processing.Processing) -> None:
    process_id = "test-adaptor-mars"
    process = proc.process(process_id)
    with pytest.raises(requests.exceptions.HTTPError, match="422 Client Error") as exc:
        process.valid_values({"invalid_param": 1})
        assert exc.response.status_code == 422  # type: ignore[attr-defined]
