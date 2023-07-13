import json
import unittest
import unittest.mock
from typing import Any

from cads_api_client import api_client, config

client_conf = {"url": "https://wikipedia.org", "key": "1337"}
env_conf = {"CADS_API_URL": "https://news.ycombinator.com/", "CADS_API_KEY": "42"}
file_conf = {"url": "https://tinyurl.com/ycku5mx4", "key": "314"}
default_conf = {"url": "http://localhost:8080/api", "key": None}


def test_client_overwrite(temp_environ: Any) -> None:
    """Client parameters overwrite environment variables, configuration file and defaults."""
    config.settings, config.config = None, None  # type: ignore
    client = api_client.ApiClient(key=client_conf["key"], url=client_conf["url"])
    assert client.url == client_conf["url"]
    assert client.key == client_conf["key"]


def test_env_overwrite(temp_environ: Any) -> None:
    """Environment variables overwrite configuration file and defaults."""
    config.settings, config.config = None, None  # type: ignore
    temp_environ["CADS_API_URL"] = env_conf["CADS_API_URL"]
    temp_environ["CADS_API_KEY"] = env_conf["CADS_API_KEY"]
    settings = config.get_settings()
    assert settings["url"] == env_conf["CADS_API_URL"]
    assert settings["key"] == env_conf["CADS_API_KEY"]


def test_file_overwrite() -> None:
    """Configuration file overwrites defaults."""
    config.settings, config.config = None, None  # type: ignore
    mock_file = unittest.mock.mock_open(read_data=json.dumps(file_conf))
    # Assign the mock file object to the built-in `open` function
    # Any code that calls `open` will use the mock file object
    with unittest.mock.patch("os.path.exists", unittest.mock.Mock(return_value=True)):
        with unittest.mock.patch("builtins.open", mock_file):
            settings = config.get_settings()
            assert settings == file_conf


def test_defaults() -> None:
    config.settings, config.config = None, None  # type: ignore
    with unittest.mock.patch("os.path.exists", unittest.mock.Mock(return_value=False)):
        settings = config.get_settings()
        assert settings == default_conf
