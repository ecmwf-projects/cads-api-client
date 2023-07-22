import json
from typing import Any

import py
import pytest

from cads_api_client import config


def test_read_configuration(tmpdir: py.path) -> None:
    expected_config = {"url": "dummy-url", "key": "dummy-key"}

    config_file = tmpdir.join(".cads-api-client.json")
    config_file.write(json.dumps(expected_config))

    res = config.read_configuration_file(str(config_file), config={})

    assert res == expected_config

    # make the file bad JSON
    config_file.write("XXX")

    with pytest.raises(ValueError):
        config.read_configuration_file(str(config_file), config={})

    with pytest.raises(FileNotFoundError):
        config.read_configuration_file("non-existent-file", config={})


def test_get_config_from_configuration_file(tmpdir: py.path) -> None:
    expected_config = {"url": "dummy-url", "key": "dummy-key"}

    config_file = tmpdir.join(".cads-api-client.json")
    config_file.write(json.dumps(expected_config))

    res = config.get_config("url", str(config_file), config={})

    assert res == expected_config["url"]

    with pytest.raises(KeyError):
        config.get_config("non-existent-key", str(config_file), config={})


def test_get_config_from_environment_variables(
    tmpdir: py.path, temp_environ: Any
) -> None:
    expected_config = {"url": "dummy-url", "key": "dummy-key"}
    file_config = {"url": "wrong-url", "key": "wrong-key"}

    config_file = tmpdir.join(".cads-api-client.json")
    config_file.write(json.dumps(file_config))

    temp_environ["CADS_API_URL"] = expected_config["url"]
    temp_environ["CADS_API_KEY"] = expected_config["key"]

    res = config.get_config("url", str(config_file), config={})

    assert res == expected_config["url"]

    res = config.get_config("key", str(config_file), config={})

    assert res == expected_config["key"]
