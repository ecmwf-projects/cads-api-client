import json
import pathlib

import pytest

from cads_api_client import config


def test_read_configuration(tmp_path: pathlib.Path) -> None:
    config_file = tmp_path / ".cads-api-client.json"
    with config_file.open("w") as fp:
        json.dump({"key": "dummy-key"}, fp)

    res = config.read_configuration_file(str(config_file), config={})

    assert res == {
        "url": "http://localhost:8080/api",
        "key": "dummy-key",
        "verify": "True",
    }


def test_read_configuration_error(tmp_path: pathlib.Path) -> None:
    config_file = tmp_path / ".cads-api-client.json"
    config_file.write_text("XXX")

    with pytest.raises(ValueError):
        config.read_configuration_file(str(config_file), config={})

    with pytest.raises(FileNotFoundError):
        config.read_configuration_file("non-existent-file", config={})


def test_get_config_from_configuration_file(tmp_path: pathlib.Path) -> None:
    expected_config = {"url": "dummy-url", "key": "dummy-key"}

    config_file = tmp_path / ".cads-api-client.json"
    with config_file.open("w") as fp:
        json.dump(expected_config, fp)

    res = config.get_config("url", str(config_file), config={})

    assert res == expected_config["url"]

    with pytest.raises(KeyError):
        config.get_config("non-existent-key", str(config_file), config={})


def test_get_config_from_environment_variables(
    tmp_path: pathlib.Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    expected_config = {"url": "dummy-url", "key": "dummy-key"}
    file_config = {"url": "wrong-url", "key": "wrong-key"}

    config_file = tmp_path / ".cads-api-client.json"
    with config_file.open("w") as fp:
        json.dump(file_config, fp)

    monkeypatch.setenv("CADS_API_URL", expected_config["url"])
    monkeypatch.setenv("CADS_API_KEY", expected_config["key"])

    res = config.get_config("url", str(config_file), config={})

    assert res == expected_config["url"]

    res = config.get_config("key", str(config_file), config={})

    assert res == expected_config["key"]
