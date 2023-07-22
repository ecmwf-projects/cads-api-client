import json
import os
from typing import Dict

CONFIG: Dict[str, str] = {}


def read_configuration_file(
    config_path: str, config: Dict[str, str] = CONFIG
) -> Dict[str, str]:
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
    config: Dict[str, str] = CONFIG,
) -> str:
    return (
        os.getenv(f"CADS_API_{key.upper()}")
        or read_configuration_file(config_path, config)[key]
    )
