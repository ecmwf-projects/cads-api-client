import pathlib

import pytest
import requests

from cads_api_client import legacy_api_client, processing


def test_retrieve(tmp_path: pathlib.Path, api_root_url: str, api_anon_key: str) -> None:
    client = legacy_api_client.LegacyApiClient(url=api_root_url, key=api_anon_key)

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


@pytest.mark.parametrize("quiet", [True, False])
def test_quiet(
    caplog: pytest.LogCaptureFixture,
    api_root_url: str,
    api_anon_key: str,
    quiet: bool,
) -> None:
    client = legacy_api_client.LegacyApiClient(
        url=api_root_url, key=api_anon_key, quiet=quiet
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
    legacy_api_client.LegacyApiClient(url=api_root_url, key=api_anon_key, debug=debug)
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
    )

    collection_id = "test-adaptor-dummy"
    request = {"size": 1}

    result = client.retrieve(collection_id, request)
    assert isinstance(result, expected_type)

    target = tmp_path / "test.grib"
    result.download(str(target))
    assert target.stat().st_size == 1
