from __future__ import annotations

import contextlib
import datetime
import os
import pathlib
import warnings
from typing import Any

import pytest
import requests
from urllib3.exceptions import InsecureRequestWarning

from cads_api_client import ApiClient

does_not_raise = contextlib.nullcontext


@pytest.fixture
def api_anon_client(api_root_url: str, api_anon_key: str) -> ApiClient:
    return ApiClient(url=api_root_url, key=api_anon_key, maximum_tries=0)


def test_accept_licence() -> None:
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        client = ApiClient(maximum_tries=0)

    if client.key is None:
        pytest.skip("The API key is missing")

    licence = client.licences["licences"][0]
    licence_id = licence["id"]
    licence_revision = licence["revision"]

    expected = {"id": licence_id, "revision": licence_revision}
    actual = client.accept_licence(licence_id, licence_revision)
    assert expected == actual

    assert any(
        licence["id"] == licence_id and licence["revision"] == licence_revision
        for licence in client.accepted_licences["licences"]
    )


def test_delete_request(api_anon_client: ApiClient) -> None:
    remote = api_anon_client.submit(
        "test-adaptor-dummy", _timestamp=datetime.datetime.now().isoformat()
    )
    reply = remote.delete()
    assert reply["status"] == "dismissed"
    with pytest.raises(requests.exceptions.HTTPError):
        remote.status


def test_check_authentication(api_root_url: str, api_anon_client: ApiClient) -> None:
    assert api_anon_client.check_authentication() == {
        "id": -1,
        "role": "anonymous",
        "sub": "anonymous",
    }

    bad_client = ApiClient(key="foo", url=api_root_url)
    with pytest.raises(requests.exceptions.HTTPError, match="401 Client Error"):
        bad_client.check_authentication()


def test_download_result(api_anon_client: ApiClient, tmp_path: pathlib.Path) -> None:
    remote = api_anon_client.submit("test-adaptor-dummy")
    target = str(tmp_path / "test.grib")

    result = api_anon_client.download_result(remote.request_uid, target)
    assert result == target
    assert os.path.exists(result)


def test_get_remote(api_anon_client: ApiClient, tmp_path: pathlib.Path) -> None:
    request_uid = api_anon_client.submit("test-adaptor-dummy").request_uid

    result = api_anon_client.get_remote(request_uid)
    assert result.request_uid == request_uid


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
        (True, pytest.raises(requests.exceptions.HTTPError, match="404 Client Error")),
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
        client.get_request(request_uid)
