from __future__ import annotations

import functools
from typing import Any, Dict, List

import attrs
import requests
from owslib import ogcapi


@attrs.define
class ApiResponse:
    response: requests.Response

    @classmethod
    def from_request(cls, *args, **kwargs):
        return cls(response=requests.request(*args, **kwargs))

    @functools.cached_property
    def json(self) -> Dict[str, Any]:
        return self.response.json()


@attrs.define
class Remote:
    url: str


@attrs.define
class Process(ApiResponse):
    def retrieve(self, **request) -> Remote:
        url = f"{self.response.request.url}/execute"
        resp = requests.post(url, data={"inputs": request})
        print(resp.json())


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
