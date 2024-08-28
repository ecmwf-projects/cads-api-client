import datetime

import pytest
import requests

from cads_api_client import ApiClient


def test_accept_licence() -> None:
    client = ApiClient()

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
