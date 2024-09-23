import pytest
from requests import HTTPError

from cads_api_client import ApiClient, config


def test_api_client_accept_licence() -> None:
    try:
        # Can not use anonymous user
        config.get_config("key")
    except Exception:
        pytest.skip("The API key is missing")

    client = ApiClient(maximum_tries=0)
    licence = client.get_licences()[0]
    licence_id = licence["id"]
    licence_revision = licence["revision"]

    expected = {"id": licence_id, "revision": licence_revision}
    actual = client.accept_licence(licence_id, licence_revision)
    assert expected == actual

    assert any(
        licence["id"] == licence_id and licence["revision"] == licence_revision
        for licence in client.accepted_licences
    )


def test_api_client_check_authentication(
    api_root_url: str, api_anon_client: ApiClient
) -> None:
    assert api_anon_client.check_authentication() == {
        "id": -1,
        "role": "anonymous",
        "sub": "anonymous",
    }

    bad_client = ApiClient(key="foo", url=api_root_url)
    with pytest.raises(HTTPError, match="401 Client Error"):
        bad_client.check_authentication()