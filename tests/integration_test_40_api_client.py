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
