import json
import os
import os.path
from typing import Dict

CONFIG_PATH = os.path.join(os.getenv("HOME", "."), ".cads-api-client.json")

settings = None  # cache settings
config = None  # cache config file


def get_config(path: str) -> Dict[str, str]:
    global config
    if config is None:
        config = {}
        if os.path.exists(path):
            with open(path, "r") as fin:
                config = json.load(fin)
    return config


def get_settings():
    global settings
    if not settings:
        settings = {
            "url": os.getenv("CADS_API_URL")
            or get_config(CONFIG_PATH).get("url")
            or "http://localhost:8080/api",
            "key": os.getenv("CADS_API_KEY") or get_config(CONFIG_PATH).get("key"),
        }
    return settings
