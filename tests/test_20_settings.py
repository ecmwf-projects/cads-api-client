import os
from importlib import reload

import cads_api_client.api_client
import cads_api_client.settings

dummy_conf = {
    "CADS_API_URL": "https://www.youtube.com/watch?v=dQw4w9WgXcQ&pp=ygUIcmlja3JvbGw%3D",
    "CADS_API_KEY": "42",
}

defaults = {"CADS_API_URL": "http://localhost:8080/api", "CADS_API_KEY": None}


# import json
# CONF_PATH = "/tmp/.cads-api-client.json"
# with open(CONF_PATH, "w") as fout:
#     json.dump(dummy_conf, fout)


def test_precedence() -> None:
    # kwargs overwrites env, file, defaults
    client = cads_api_client.api_client.ApiClient(
        key=dummy_conf["CADS_API_KEY"], url=dummy_conf["CADS_API_URL"]
    )
    assert client.url == dummy_conf["CADS_API_URL"]
    assert client.key == dummy_conf["CADS_API_KEY"]

    # env overwrites file, defaults
    os.environ["CADS_API_URL"] = dummy_conf["CADS_API_URL"]
    os.environ["CADS_API_KEY"] = dummy_conf["CADS_API_KEY"]
    reload(cads_api_client.settings)
    reload(cads_api_client.api_client)
    client = cads_api_client.api_client.ApiClient()
    assert client.url == dummy_conf["CADS_API_URL"]
    assert client.key == dummy_conf["CADS_API_KEY"]
    os.environ.pop("CADS_API_URL")
    os.environ.pop("CADS_API_KEY")

    # TODO file overwrites defaults

    # defaults
    reload(cads_api_client.settings)
    reload(cads_api_client.api_client)
    client = cads_api_client.api_client.ApiClient()
    assert client.url == defaults["CADS_API_URL"]
    assert client.key == defaults["CADS_API_KEY"]
