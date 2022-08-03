from __future__ import annotations

import functools
from typing import Any, Dict, List, Optional, Type, TypeVar

import attrs
import requests
import xarray as xr
from owslib import ogcapi

T_ApiResponse = TypeVar("T_ApiResponse", bound="ApiResponse")


@attrs.define(slots=False)
class ApiResponse:
    response: requests.Response

    @classmethod
    def from_request(
        cls: Type[T_ApiResponse], *args: Any, **kwargs: Any
    ) -> T_ApiResponse:
        return cls(requests.request(*args, **kwargs))

    @functools.cached_property
    def json(self) -> Dict[str, Any]:
        return self.response.json()  # type: ignore

    def get_links(self, rel: Optional[str] = None) -> List[Dict[str, str]]:
        links = []
        for link in self.json.get("links", []):
            if rel is not None and link.get("rel") == rel:
                links.append(link)
        return links

    def get_links_hrefs(self, **kwargs: str) -> List[str]:
        return [link["href"] for link in self.get_links(**kwargs) if "href" in link]


@attrs.define
class ProcessList(ApiResponse):
    def process_ids(self) -> List[str]:
        return [proc["id"] for proc in self.json["processes"]]


@attrs.define
class Process(ApiResponse):
    def execute(self, **inputs: Any) -> Remote:
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
        return json["status"]  # type: ignore

    def wait_on_result_ready(self) -> None:
        pass

    def download_result(self) -> None:
        pass

    def to_grib(self, path: str) -> None:
        pass

    def to_dataset(self) -> xr.Dataset:
        return xr.Dataset()


@attrs.define
class StatusInfo(ApiResponse):
    pass


@attrs.define
class JobList(ApiResponse):
    def job_ids(self) -> List[str]:
        return [job["id"] for job in self.json["jobs"]]


@attrs.define
class Results(ApiResponse):
    pass


class Processing(ogcapi.API):  # type: ignore
    supported_api_version = "v1"

    def __init__(self, url: str, *args: Any, **kwargs: Any) -> None:
        url = f"{url}/{self.supported_api_version}"
        super().__init__(url, *args, **kwargs)

    def processes(self) -> ProcessList:
        url = self._build_url("processes")
        return ProcessList.from_request("get", url)

    def process(self, process_id: str) -> Process:
        url = self._build_url(f"processes/{process_id}")
        return Process.from_request("get", url)

    def process_execute(self, process_id: str, **inputs: Any) -> StatusInfo:
        url = self._build_url(f"processes/{process_id}/execute")
        return StatusInfo.from_request("post", url, json={"inputs": inputs})

    def jobs(self) -> JobList:
        url = self._build_url("jobs")
        return JobList.from_request("get", url)

    def job(self, job_id: str) -> StatusInfo:
        url = self._build_url(f"jobs/{job_id}")
        return StatusInfo.from_request("get", url)

    def job_results(self, job_id: str) -> Results:
        url = self._build_url(f"jobs/{job_id}/results")
        return Results.from_request("get", url)
