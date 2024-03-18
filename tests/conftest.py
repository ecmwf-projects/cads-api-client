import os
from typing import Any

import pytest
import requests


@pytest.fixture
def api_root_url() -> str:
    from cads_api_client import config

    return config.get_config("url")


@pytest.fixture
def api_key(api_root_url: str) -> str:
    if key := os.getenv("CADS_API_KEY"):
        return key

    # default to test user 1
    key = "00000000-0000-4000-a000-000000000000"

    # Accept all licences
    result = requests.get(f"{api_root_url}/catalogue/v1/vocabularies/licences")
    requests.patch(
        f"{api_root_url}/profiles/v1/account/licences",
        json=result.json(),
        headers={"PRIVATE-TOKEN": key},
    )
    return key


@pytest.fixture
def api_key_anon() -> str:
    return "00112233-4455-6677-c899-aabbccddeeff"


@pytest.fixture
def request_year() -> str:
    return os.environ.get("CADS_TEST_YEAR", "2016")


@pytest.fixture()
def temp_environ() -> Any:
    """Create a modifiable environment that affect only the test scope."""
    old_environ = dict(os.environ)

    yield os.environ

    os.environ.clear()
    os.environ.update(old_environ)
