import os

import pytest


@pytest.fixture
def api_root_url() -> str:
    return os.environ.get("CADS_API_ROOT_URL", "http://localhost:8080/api")


@pytest.fixture
def api_key() -> str:
    # default to anonymous access
    return os.environ.get("CADS_API_KEY", "00112233-4455-6677-c899-aabbccddeeff")


@pytest.fixture
def request_year() -> str:
    return os.environ.get("CADS_YEAR", "2016")
