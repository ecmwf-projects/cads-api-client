from __future__ import annotations

import json
import os

CONFIG: dict[str, str] = {}
SUPPORTED_API_VERSION = "v1"


def read_configuration_file(
    config_path: str, config: dict[str, str] = CONFIG
) -> dict[str, str]:
    if not config:
        # Default config
        config["url"] = "http://localhost:8080/api"
        config["verify"] = "True"
        config_path = os.path.expanduser(config_path)
        try:
            with open(config_path) as fin:
                config.update(json.load(fin))
        except FileNotFoundError:
            raise
        except Exception:
            raise ValueError(f"failed to parse {config_path!r} file")
    return config


def get_config(
    key: str,
    config_path: str | None = None,
    config: dict[str, str] = CONFIG,
) -> str:
    if config_path is None:
        config_path = os.getenv("CADS_API_RC", "~/.cads-api-client.json")
    return (
        os.getenv(f"CADS_API_{key.upper()}")
        or read_configuration_file(config_path, config)[key]
    )
