import os

import pytest


@pytest.fixture
def api_root_url() -> str:
    from cads_api_client import api_client

    return api_client.CADS_API_URL


@pytest.fixture
def api_key() -> str:
    from cads_api_client import api_client

    # default to anonymous access for tests
    return api_client.CADS_API_KEY or "00112233-4455-6677-c899-aabbccddeeff"


@pytest.fixture
def request_year() -> str:
    return os.environ.get("CADS_TEST_YEAR", "2016")
