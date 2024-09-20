from __future__ import annotations

import contextlib
import os
import pathlib
from typing import Any

import pytest
import requests
from urllib3.exceptions import InsecureRequestWarning

from cads_api_client import ApiClient, catalogue, config, processing

does_not_raise = contextlib.nullcontext


def test_api_client_apply_constraints(api_anon_client: ApiClient) -> None:
    result = api_anon_client.apply_constraints(
        "test-adaptor-url", version="deprecated (1.0)"
    )
    assert result["reference_dataset"] == ["cru", "cru_and_gpcc"]

    with pytest.raises(requests.HTTPError, match="invalid param 'foo'"):
        api_anon_client.apply_constraints("test-adaptor-url", foo="bar")


def test_api_client_check_authentication(
    api_root_url: str, api_anon_client: ApiClient
) -> None:
    assert api_anon_client.check_authentication() == {
        "id": -1,
        "role": "anonymous",
        "sub": "anonymous",
    }

    bad_client = ApiClient(key="foo", url=api_root_url)
    with pytest.raises(requests.HTTPError, match="401 Client Error"):
        bad_client.check_authentication()


def test_api_client_download_results(
    api_anon_client: ApiClient, tmp_path: pathlib.Path
) -> None:
    remote = api_anon_client.submit("test-adaptor-dummy")
    target = str(tmp_path / "test.grib")

    result = api_anon_client.download_results(remote.request_uid, target)
    assert result == target
    assert os.path.exists(result)


def test_api_client_estimate_costs(api_anon_client: ApiClient) -> None:
    result = api_anon_client.estimate_costs(
        "test-layout-sandbox-nogecko-dataset", size=100
    )
    assert result["cost"] == 100


def test_api_client_get_collection(api_anon_client: ApiClient) -> None:
    collection = api_anon_client.get_collection("test-adaptor-dummy")
    assert isinstance(collection, catalogue.Collection)
    assert collection.id == "test-adaptor-dummy"


def test_api_client_get_job(api_anon_client: ApiClient) -> None:
    remote = api_anon_client.submit("test-adaptor-dummy")
    job = api_anon_client.get_job(remote.request_uid)
    assert isinstance(job, processing.StatusInfo)


def test_api_client_get_process(api_anon_client: ApiClient) -> None:
    process = api_anon_client.get_process("test-adaptor-dummy")
    assert isinstance(process, processing.Process)
    assert process.id == "test-adaptor-dummy"


def test_api_client_get_remote(api_anon_client: ApiClient) -> None:
    request_uid = api_anon_client.submit("test-adaptor-dummy").request_uid
    remote = api_anon_client.get_remote(request_uid)
    assert remote.request_uid == request_uid


def test_api_client_retrieve(
    api_anon_client: ApiClient,
    tmp_path: pathlib.Path,
) -> None:
    expected_target = str(tmp_path / "dummy.grib")
    actual_target = api_anon_client.retrieve(
        "test-adaptor-dummy", target=expected_target, size=1
    )
    assert expected_target == actual_target
    assert os.path.getsize(actual_target) == 1


def test_api_client_submit(api_anon_client: ApiClient) -> None:
    remote = api_anon_client.submit("test-adaptor-dummy")
    assert isinstance(remote, processing.Remote)


def test_api_client_submit_and_wait_on_results(api_anon_client: ApiClient) -> None:
    results = api_anon_client.submit_and_wait_on_results("test-adaptor-dummy")
    assert isinstance(results, processing.Results)


def test_api_client_collections(api_anon_client: ApiClient) -> None:
    assert isinstance(api_anon_client.collections, catalogue.Collections)


def test_api_client_jobs(api_anon_client: ApiClient) -> None:
    assert isinstance(api_anon_client.jobs, processing.JobList)


def test_api_client_licences(api_anon_client: ApiClient) -> None:
    licences = api_anon_client.licences
    assert licences
    assert all("id" in licence and "revision" in licence for licence in licences)


def test_api_client_processes(api_anon_client: ApiClient) -> None:
    assert isinstance(api_anon_client.processes, processing.ProcessList)


def test_api_client_accept_licence() -> None:
    try:
        # Can not use anonymous user
        config.get_config("key")
    except Exception:
        pytest.skip("The API key is missing")

    client = ApiClient(maximum_tries=0)
    licence = client.licences[0]
    licence_id = licence["id"]
    licence_revision = licence["revision"]

    expected = {"id": licence_id, "revision": licence_revision}
    actual = client.accept_licence(licence_id, licence_revision)
    assert expected == actual

    assert any(
        licence["id"] == licence_id and licence["revision"] == licence_revision
        for licence in client.accepted_licences
    )


def test_api_client_verify(
    api_root_url: str,
    api_anon_key: str,
    tmp_path: pathlib.Path,
) -> None:
    insecure_client = ApiClient(
        url=api_root_url, key=api_anon_key, verify=False, maximum_tries=0
    )
    with pytest.warns(InsecureRequestWarning):
        insecure_client.retrieve(
            "test-adaptor-dummy", target=str(tmp_path / "test.grib")
        )


def test_api_client_timeout(
    api_root_url: str,
    api_anon_key: str,
    tmp_path: pathlib.Path,
) -> None:
    client = ApiClient(url=api_root_url, key=api_anon_key, timeout=0)
    with pytest.raises(ValueError, match="timeout"):
        client.retrieve("test-adaptor-dummy", target=str(tmp_path / "test.grib"))


@pytest.mark.parametrize("progress", [True, False])
def test_api_client_progress(
    api_root_url: str,
    api_anon_key: str,
    tmp_path: pathlib.Path,
    progress: bool,
    capsys: pytest.CaptureFixture[str],
) -> None:
    with capsys.disabled():
        client = ApiClient(
            url=api_root_url, key=api_anon_key, progress=progress, maximum_tries=0
        )
        submitted = client.submit("test-adaptor-dummy")
    submitted.download(target=str(tmp_path / "test.grib"))
    captured = capsys.readouterr()
    assert captured.err if progress else not captured.err


@pytest.mark.parametrize(
    "cleanup,raises",
    [
        (True, pytest.raises(requests.HTTPError, match="404 Client Error")),
        (False, does_not_raise()),
    ],
)
def test_api_client_cleanup(
    api_root_url: str,
    api_anon_key: str,
    cleanup: bool,
    raises: contextlib.nullcontext[Any],
) -> None:
    client = ApiClient(
        url=api_root_url, key=api_anon_key, cleanup=cleanup, maximum_tries=0
    )
    remote = client.submit("test-adaptor-dummy")
    request_uid = remote.request_uid
    del remote

    client = ApiClient(url=api_root_url, key=api_anon_key, maximum_tries=0)
    with raises:
        client.get_remote(request_uid)
