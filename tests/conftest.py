import os

import pytest


@pytest.fixture
def dev_env_api_url() -> str:
    return os.environ.get("CADS_API_ROOT_URL", "http://localhost:8080/api")
