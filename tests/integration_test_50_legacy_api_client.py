import pathlib

import pytest

from cads_api_client import legacy_api_client


def test_retrieve(tmp_path: pathlib.Path, api_root_url: str, api_key: str) -> None:
    client = legacy_api_client.LegacyApiClient(url=api_root_url, key=api_key)

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


@pytest.mark.parametrize("quiet", [True, False])
def test_quiet(
    caplog: pytest.LogCaptureFixture, api_root_url: str, api_key: str, quiet: bool
) -> None:
    client = legacy_api_client.LegacyApiClient(
        url=api_root_url, key=api_key, quiet=quiet
    )
    client.retrieve("test-adaptor-dummy", {})
    records = [record for record in caplog.records if record.levelname == "INFO"]
    assert not records if quiet else records


@pytest.mark.parametrize("debug", [True, False])
def test_debug(
    caplog: pytest.LogCaptureFixture, api_root_url: str, api_key: str, debug: bool
) -> None:
    legacy_api_client.LegacyApiClient(url=api_root_url, key=api_key, debug=debug)
    records = [record for record in caplog.records if record.levelname == "DEBUG"]
    assert records if debug else not records
