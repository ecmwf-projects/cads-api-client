from __future__ import annotations

import functools
from typing import Any, Dict, List

import attrs
import requests
import xarray as xr
from owslib import ogcapi


@attrs.define(slots=False)
class ApiResponse:
    response: requests.Response

    @classmethod
    def from_request(cls, *args, **kwargs):
        return cls(requests.request(*args, **kwargs))

    @functools.cached_property
    def json(self) -> Dict[str, Any]:
        return self.response.json()

    def get_links(self, rel=None) -> List[Dict[str, str]]:
        links = []
        for link in self.json.get("links", []):
            if rel is not None and link.get("rel") == rel:
                links.append(link)
        return links

    def get_links_hrefs(self, **kwargs) -> List[str]:
        return [link["href"] for link in self.get_links(**kwargs) if "href" in link]


@attrs.define
class ProcessList(ApiResponse):
    def process_ids(self) -> List[str]:
        return [proc["id"] for proc in self.json["processes"]]


@attrs.define
class Process(ApiResponse):
    def execute(self, **inputs) -> Remote:
        url = f"{self.response.request.url}/execute"
        resp = ApiResponse(requests.post(url, json={"inputs": inputs}))
        hrefs = resp.get_links_hrefs(rel="monitor")
        if len(hrefs) != 1:
            raise RuntimeError("monitor URL not found or not unique")
        return Remote(hrefs[0])


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


@attrs.define
class JobStatus(ApiResponse):
    pass


class Processing(ogcapi.API):  # type: ignore
    supported_api_version = "v1"

    def __init__(self, url, *args, **kwargs):
        url = f"{url}/{self.supported_api_version}"
        return super().__init__(url, *args, **kwargs)

    def processes(self) -> ProcessList:
        url = self._build_url("processes")
        return ProcessList.from_request("get", url)

    def process(self, process_id: str) -> Dict[str, Any]:
        url = self._build_url(f"processes/{process_id}")
        return Process.from_request("get", url)

    def make_remote(self, request_uid: str) -> Remote:
        url = f"{self.url}/jobs/{request_uid}"
        return Remote(url)

    def job(self, job_id: str) -> Dict[str, Any]:
        url = self._build_url(f"jobs/{job_id}")
        return JobStatus.from_request("get", url)
