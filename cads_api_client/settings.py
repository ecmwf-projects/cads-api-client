import functools
import json
import os
import os.path

CONF_FN = ".cads-api-client.json"
CONF_PATH = os.path.join(os.getenv("HOME", "."), CONF_FN)


@functools.lru_cache
def getconf(path) -> dict:
    config = {}
    if os.path.exists(path):
        with open(path, "r") as fin:
            config = json.load(fin)
    return config


CADS_API_URL = (
    os.getenv("CADS_API_URL")
    or getconf(CONF_PATH).get("CADS_API_URL")
    or "http://cds2-dev.copernicus-climate.eu/api"
)
CADS_API_KEY = (
    os.getenv("CADS_API_KEY")
    or getconf(CONF_PATH).get("CADS_API_KEY")
    or "00112233-4455-6677-c899-aabbccddeeff"
)

# TODO pydantic-settings?
