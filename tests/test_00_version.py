import cads_api_client


def test_version() -> None:
    assert cads_api_client.__version__ != "999"
