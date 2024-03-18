import pathlib

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
