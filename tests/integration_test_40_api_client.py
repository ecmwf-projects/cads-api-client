import pytest

from cads_api_client import ApiClient


@pytest.fixture
def api_client(api_root_url: str, api_key: str) -> ApiClient:
    return ApiClient(url=api_root_url, key=api_key)


def test_accept_licence(api_client: ApiClient) -> None:
    licence = api_client.licences["licences"][0]
    licence_id = licence["id"]
    licence_revision = licence["revision"]

    expected = {"id": licence_id, "revision": licence_revision}
    actual = api_client.accept_licence(licence_id, licence_revision)
    assert expected == actual

    assert any(
        [
            licence["id"] == licence_id and licence["revision"] == licence_revision
            for licence in api_client.accepted_licences["licences"]
        ]
    )
