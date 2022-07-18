import pytest


@pytest.fixture
def dev_env_api_url() -> str:
    return "http://localhost:8080/api"
