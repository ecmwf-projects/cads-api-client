import pytest
import requests

from cads_api_client import processes, APIClient


def test_processes(api_root_url: str) -> None:
    client = APIClient(base_url=api_root_url)
    procs = client.processes()
    first_proc = next(procs)
    assert isinstance(first_proc, processes.Process)
    # assert "processes" in first_process.json
    # assert isinstance(res.json["processes"], list)
    # assert "links" in res.json
    # assert isinstance(res.json["links"], list)
    # assert len(res.process_ids()) == 10


def test_processes_limit(api_root_url: str) -> None:
    client = APIClient(base_url=api_root_url)
    procs = client.processes(params={"limit": 1})
    procs_first_page = list(procs)
    # if res is not None:
    #     assert res.response.status_code == 200
    # if res is not None:
    #     assert isinstance(res.json(), dict)
    expected_page_len = 70
    assert len(procs_first_page) == expected_page_len


def test_process(api_root_url: str) -> None:
    process_id = "test-adaptor-dummy"
    res = processes.Process(base_url=api_root_url, pid=process_id)

    assert isinstance(res, processes.Process)
    assert res.id == process_id
    assert "links" in res.json()
    assert isinstance(res.json()["links"], list)


def test_validate_constraints(api_root_url: str) -> None:
    process_id = "test-adaptor-mars"
    process = processes.Process(pid=process_id)
    res = process.valid_values({})
    assert set(["product_type", "variable", "year", "month", "time"]) <= set(res)


# TODO move to jobs
def test_collection_missing_licence(
    api_root_url: str, api_key: str, request_year: str
) -> None:
    collection_id = "test-adaptor-mars"
    process = processes.Process(collection_id, base_url=api_root_url, api_key=api_key)

    with pytest.raises(requests.exceptions.HTTPError, match="403 Client Error"):
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


# TODO move to jobs
def test_jobs_list(api_root_url: str, api_key: str, request_year: str) -> None:
    collection_id = "test-adaptor-dummy"
    # headers = {"PRIVATE-TOKEN": api_key}
    # proc = processing.Processing(f"{api_root_url}/retrieve", headers=headers)
    # process = proc.process(collection_id)
    client = APIClient(base_url=api_root_url, api_key=api_key)
    process = processes.Process(pid=collection_id)

    _ = process.execute(inputs={})
    _ = process.execute(inputs={})

    # res = proc.jobs().json
    # assert len(res["jobs"]) >= 2
    res = list(client.jobs())
    assert len(res) >= 2

    # res = client.jobs(params={"limit": 1}).json
    # assert len(res["jobs"]) == 1
    jobs = list(client.jobs(params={"limit": 1}))
    assert len(jobs) == 1

    # jobs = proc.jobs(params={"limit": 1})
    # res = jobs.next().json  # type: ignore
    jobs = client.jobs(params={"limit": 1})
    res = next(jobs).json()

    assert res is not None
    assert len(res["jobs"]) == 1

def test_validate_constraints_error(api_root_url: str) -> None:
    process_id = "test-adaptor-mars"
    # proc = processing.Processing(f"{api_root_url}/retrieve")
    # process = proc.process(process_id)
    process = processes.Process(base_url=api_root_url, pid=process_id)
    with pytest.raises(RuntimeError, match="422 Client Error"):
        process.valid_values({"invalid_param": 1})

