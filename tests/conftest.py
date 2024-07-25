from __future__ import annotations

import os
from typing import TYPE_CHECKING, Any

import pytest

if TYPE_CHECKING:
    from cads_api_client import ApiClient


@pytest.fixture
def api_root_url() -> str:
    from cads_api_client import config

    return config.get_config("url")


@pytest.fixture
def api_key_test() -> str:
    key = os.getenv("CADS_TEST_KEY")
    if key is None:
        pytest.skip("CADS_TEST_KEY is not defined")
    return key


@pytest.fixture
def api_key_anon() -> str:
    return os.getenv("CADS_ANON_KEY", "00112233-4455-6677-c899-aabbccddeeff")


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


@pytest.fixture
def api_client(api_root_url: str, api_key_test: str) -> ApiClient:
    from cads_api_client import ApiClient

    return ApiClient(url=api_root_url, key=api_key_test)
