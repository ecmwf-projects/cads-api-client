from __future__ import annotations

import json
import os
from typing import Any

SUPPORTED_API_VERSION = "v1"


def read_configuration_file(config_path: str | None = None) -> dict[Any, Any]:
    if config_path is None:
        config_path = os.getenv("CADS_API_RC", "~/.cads-api-client.json")
    config_path = os.path.expanduser(config_path)
    try:
        with open(config_path) as fin:
            config = json.load(fin)
        assert isinstance(config, dict)
    except FileNotFoundError:
        raise
    except Exception:
        raise ValueError(f"failed to parse {config_path!r} file")
    return config


def get_config(key: str, config_path: str | None = None) -> Any:
    return (
        os.getenv(f"CADS_API_{key.upper()}")
        or read_configuration_file(config_path)[key]
    )
