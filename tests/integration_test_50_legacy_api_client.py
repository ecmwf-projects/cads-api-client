from __future__ import annotations

import contextlib
import pathlib
import time
from typing import Any

import pytest
import requests

from cads_api_client import legacy_api_client, processing

does_not_raise = contextlib.nullcontext


def legacy_update(remote: processing.Remote) -> None:
    # See https://github.com/ecmwf/cdsapi/blob/master/examples/example-era5-update.py
    sleep = 1
    while True:
        with pytest.deprecated_call():
            remote.update()

        reply = remote.reply
        remote.info("Request ID: %s, state: %s" % (reply["request_id"], reply["state"]))

        if reply["state"] == "completed":
            break
        elif reply["state"] in ("queued", "running"):
            remote.info("Request ID: %s, sleep: %s", reply["request_id"], sleep)
            time.sleep(sleep)
        elif reply["state"] in ("failed",):
            remote.error("Message: %s", reply["error"].get("message"))
            remote.error("Reason:  %s", reply["error"].get("reason"))
            for n in (
                reply.get("error", {})
                .get("context", {})
                .get("traceback", "")
                .split("\n")
            ):
                if n.strip() == "":
                    break
                remote.error("  %s", n)
            raise Exception(
                "%s. %s."
                % (reply["error"].get("message"), reply["error"].get("reason"))
            )


def test_retrieve(tmp_path: pathlib.Path, api_root_url: str, api_anon_key: str) -> None:
    client = legacy_api_client.LegacyApiClient(
        url=api_root_url, key=api_anon_key, retry_max=0
    )

    collection_id = "test-adaptor-dummy"
    request = {"size": 1}

    target = tmp_path / "test-retrieve-with-target.grib"
    actual_target = client.retrieve(collection_id, request, str(target))
    assert str(target) == actual_target
    assert target.stat().st_size == 1

    result = client.retrieve(collection_id, request)
    target = tmp_path / "test-retrieve-no-target.grib"
    actual_target = result.download(str(target))
    assert str(target) == actual_target
    assert target.stat().st_size == 1

    response = requests.head(result.location)
    assert response.status_code == 200

    assert result.content_length == 1
    assert result.content_type == "application/x-grib"


@pytest.mark.parametrize("quiet", [True, False])
def test_quiet(
    caplog: pytest.LogCaptureFixture,
    api_root_url: str,
    api_anon_key: str,
    quiet: bool,
) -> None:
    client = legacy_api_client.LegacyApiClient(
        url=api_root_url, key=api_anon_key, quiet=quiet, retry_max=0
    )
    client.retrieve("test-adaptor-dummy", {})
    records = [record for record in caplog.records if record.levelname == "INFO"]
    assert not records if quiet else records


@pytest.mark.parametrize("debug", [True, False])
def test_debug(
    caplog: pytest.LogCaptureFixture,
    api_root_url: str,
    api_anon_key: str,
    debug: bool,
) -> None:
    legacy_api_client.LegacyApiClient(
        url=api_root_url, key=api_anon_key, debug=debug, retry_max=0
    )
    records = [record for record in caplog.records if record.levelname == "DEBUG"]
    assert records if debug else not records


@pytest.mark.parametrize(
    "wait_until_complete,expected_type",
    [(True, processing.Results), (False, processing.Remote)],
)
def test_wait_until_complete(
    tmp_path: pathlib.Path,
    api_root_url: str,
    api_anon_key: str,
    wait_until_complete: bool,
    expected_type: type,
) -> None:
    client = legacy_api_client.LegacyApiClient(
        url=api_root_url,
        key=api_anon_key,
        wait_until_complete=wait_until_complete,
        retry_max=0,
    )

    collection_id = "test-adaptor-dummy"
    request = {"size": 1}

    result = client.retrieve(collection_id, request)
    assert isinstance(result, expected_type)

    target = tmp_path / "test.grib"
    result.download(str(target))
    assert target.stat().st_size == 1


@pytest.mark.parametrize(
    "collection_id,raises",
    [
        ("test-adaptor-dummy", does_not_raise()),
        ("test-adaptor-mars", pytest.raises(Exception, match="400 Client Error")),
    ],
)
def test_legacy_update(
    api_root_url: str,
    api_anon_key: str,
    collection_id: str,
    raises: contextlib.nullcontext[Any],
) -> None:
    client = legacy_api_client.LegacyApiClient(
        url=api_root_url, key=api_anon_key, wait_until_complete=False, retry_max=0
    )
    remote = client.retrieve(collection_id, {})
    assert isinstance(remote, processing.Remote)
    with raises:
        legacy_update(remote)


def test_legacy_api_client_kwargs(api_root_url: str, api_anon_key: str) -> None:
    session = requests.Session()
    client = legacy_api_client.LegacyApiClient(
        url=api_root_url,
        key=api_anon_key,
        verify=0,
        timeout=1,
        progress=False,
        delete=True,
        retry_max=2,
        sleep_max=3,
        wait_until_complete=False,
        session=session,
    )
    assert client.client.url == api_root_url
    assert client.client.key == api_anon_key
    assert client.client.verify is False
    assert client.timeout == 1
    assert client.client.progress is False
    assert client.client.cleanup is True
    assert client.client.maximum_tries == 2
    assert client.client.sleep_max == 3


def test_legacy_api_client_error(
    api_root_url: str,
    api_anon_key: str,
) -> None:
    with pytest.raises(ValueError, match="Wrong parameters: {'foo'}"):
        legacy_api_client.LegacyApiClient(url=api_root_url, key=api_anon_key, foo="bar")
