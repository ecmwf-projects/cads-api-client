import functools
import json
import os
import os.path

from pydantic import AnyUrl
from pydantic_settings import BaseSettings

CONF_FN = ".cads-api-client.json"
CONF_PATH = os.path.join(os.getenv("HOME", "."), CONF_FN)


@functools.lru_cache
def getconf(path) -> dict:
    config = {}
    if os.path.exists(path):
        with open(path, "r") as fin:
            config = json.load(fin)
    return config


def getvar(name, default_value):
    return os.getenv(name) or getconf(CONF_PATH).get(name) or default_value


# TODO tests
class Settings(BaseSettings):
    api_url: AnyUrl = getvar(
        "CADS_API_URL", "http://cds2-dev.copernicus-climate.eu/api"
    )
    api_key: str = getvar("CADS_API_KEY", "00112233-4455-6677-c899-aabbccddeeff")


# note that pydantic settings does not read from json files (but from .env files)
# plus we need to keep the hierarchy


if __name__ == "__main__":
    settings = Settings()
