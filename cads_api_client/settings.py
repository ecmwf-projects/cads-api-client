import functools
import json
import os
import os.path

CONFIG_FILENAME = ".cads-api-client.json"
CONFIG_PATH = os.path.join(os.getenv("HOME", "."), CONFIG_FILENAME)


@functools.lru_cache
def get_config(path) -> dict:
    config = {}
    if os.path.exists(path):
        with open(path, "r") as fin:
            config = json.load(fin)
    return config


CADS_API_URL = (
    os.getenv("CADS_API_URL")
    or get_config(CONFIG_PATH).get("CADS_API_URL")
    or "http://localhost:8080/api"
)
CADS_API_KEY = os.getenv("CADS_API_KEY") or get_config(CONFIG_PATH).get("CADS_API_KEY")
