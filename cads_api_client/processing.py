from __future__ import annotations

import functools
from typing import Any, Dict, List

import attrs
import requests
import xarray as xr
from owslib import ogcapi


@attrs.define
class ApiResponse:
    response: requests.Response

    @classmethod
    def from_request(cls, *args, **kwargs):
        return cls(requests.request(*args, **kwargs))

    @functools.cached_property
    def json(self) -> Dict[str, Any]:
        return self.response.json()


@attrs.define(slots=False)
class Remote:
    url: str

    @functools.cached_property
    def request_uid(self) -> str:
        return self.url.rpartition("/")[2]

    @property
    def status(self) -> str:
        # TODO: cache responses for a timeout (possibly reported nby the server)
        json = requests.get(self.url).json()
        return json["status"]

    def wait_on_result_ready(self):
        pass

    def download_result(self):
        pass

    def to_grib(self, path) -> None:
        pass

    def to_dataset(self) -> xr.Dataset:
        return xr.Dataset()


@attrs.define(slots=False)
class Process(ApiResponse):
    def execute(self, **inputs) -> Remote:
        url = f"{self.response.request.url}/execute"
        resp = requests.post(url, json={"inputs": inputs})
        json = resp.json()
        if "links" not in json:
            raise ValueError(json)
        for link in json["links"]:
            if link.get("rel") == "monitor":
                return Remote(link["href"])
        raise ValueError


class Processing(ogcapi.API):  # type: ignore
    def processes(self) -> Dict[str, Any]:
        path = "processes"
        processes = self._request(path)
        assert isinstance(processes, dict)
        return processes

    def process_ids(self) -> List[str]:
        processes = self.processes()
        ids = [proc["id"] for proc in processes["processes"]]
        return ids

    def process(self, process_id: str) -> Dict[str, Any]:
        url = self._build_url(f"processes/{process_id}")
        return Process.from_request("get", url)

    def make_remote(self, request_uid: str) -> Remote:
        return Remote(self, request_uid)


def get_process_id_from_links(links: List[Dict[str, str]]) -> str:
    for link in links:
        if link.get("rel") == "retrieve-process":
            href = link.get("href", "")
            api_url, _, process_id = href.rpartition("/processes/")
            if process_id == "":
                raise RuntimeError(f"Can not parse link href {href}")
            return process_id
    raise RuntimeError('No link with rel="retrieve-process"')
