from __future__ import annotations

import os
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    pass


@pytest.fixture
def api_root_url() -> str:
    from cads_api_client import config

    try:
        return str(config.get_config("url"))
    except Exception:
        return "http://localhost:8080/api"


@pytest.fixture
def api_anon_key() -> str:
    return os.getenv("CADS_API_ANON_KEY", "00112233-4455-6677-c899-aabbccddeeff")
