from __future__ import annotations

import os
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from cads_api_client import ApiClient


@pytest.fixture
def api_root_url() -> str:
    from cads_api_client import config

    return config.get_config("url")


@pytest.fixture
def api_anon_key() -> str:
    return os.getenv("CADS_API_ANON_KEY", "00112233-4455-6677-c899-aabbccddeeff")


@pytest.fixture
def api_anon_client(api_root_url: str, api_anon_key: str) -> ApiClient:
    from cads_api_client import ApiClient

    return ApiClient(url=api_root_url, key=api_anon_key)
