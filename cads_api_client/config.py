from __future__ import annotations

import json
import os

CONFIG: dict[str, str] = {}


def read_configuration_file(
    config_path: str, config: dict[str, str] = CONFIG
) -> dict[str, str]:
    if not config:
        # Default config
        config["url"] = "http://localhost:8080/api"
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
    config_path: str = "~/.cads-api-client.json",
    config: dict[str, str] = CONFIG,
) -> str:
    return (
        os.getenv(f"CADS_API_{key.upper()}")
        or read_configuration_file(config_path, config)[key]
    )
