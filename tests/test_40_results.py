import os
import pathlib

import pytest
import responses

from cads_api_client import Results

RESULTS_URL = "http://localhost:8080/api/retrieve/v1/jobs/9bfc1362-2832-48e1-a235-359267420bb2/results"
RESULTS_JSON = {
    "asset": {
        "value": {
            "type": "application/x-grib",
            "href": "http://httpbin.org/bytes/1",
            "file:size": 1,
        }
    }
}


@pytest.fixture
def results() -> Results:
    with responses.RequestsMock() as rsps:
        rsps.add(
            responses.GET,
            RESULTS_URL,
            json=RESULTS_JSON,
            status=200,
            content_type="application/json",
        )
        results = Results.from_request(
            "get",
            RESULTS_URL,
            headers={},
            session=None,
            retry_options={"maximum_tries": 1},
            request_options={},
            download_options={},
            sleep_max=120,
            cleanup=False,
            log_callback=None,
        )
    return results


@pytest.mark.parametrize("target", ("dummy.grib", None))
def test_results_download(
    monkeypatch: pytest.MonkeyPatch,
    results: Results,
    tmp_path: pathlib.Path,
    target: str | None,
) -> None:
    monkeypatch.chdir(tmp_path)
    actual_target = results.download(target=target)
    assert actual_target == target or "1"
    assert os.path.getsize(actual_target) == 1


def test_results_asset(results: Results) -> None:
    assert results.asset == {
        "file:size": 1,
        "href": "http://httpbin.org/bytes/1",
        "type": "application/x-grib",
    }


def test_results_content_length(results: Results) -> None:
    assert results.content_length == 1


def test_results_content_type(results: Results) -> None:
    assert results.content_type == "application/x-grib"


def test_results_json(results: Results) -> None:
    assert results.json == RESULTS_JSON


def test_results_location(results: Results) -> None:
    assert results.location == "http://httpbin.org/bytes/1"


def test_results_url(results: Results) -> None:
    assert results.url == RESULTS_URL
