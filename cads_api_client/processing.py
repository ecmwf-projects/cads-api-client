from __future__ import annotations

import functools
import logging
import os
import time
import urllib
from typing import Any, Dict, List, Optional, Type, TypeVar

import attrs
import multiurl
import requests

T_ApiResponse = TypeVar("T_ApiResponse", bound="ApiResponse")

logger = logging.Logger(__name__)


class ProcessingFailedError(RuntimeError):
    pass


class DownloadError(RuntimeError):
    pass


@attrs.define(slots=False)
class ApiResponse:
    response: requests.Response

    @classmethod
    def from_request(
        cls: Type[T_ApiResponse],
        *args: Any,
        **kwargs: Any,
    ) -> T_ApiResponse:
        # TODO:  use HTTP session
        response = requests.request(*args, **kwargs)
        response.raise_for_status()
        self = cls(response)
        return self

    @classmethod
    def from_request_robust(
        cls: Type[T_ApiResponse],
        *args: Any,
        retry_options: Dict[str, Any] = {},
        **kwargs: Any,
    ) -> T_ApiResponse:
        # TODO:  use HTTP session
        response = multiurl.robust(requests.request, **retry_options)(*args, **kwargs)
        response.raise_for_status()
        self = cls(response)
        return self

    @functools.cached_property
    def json(self) -> Dict[str, Any]:
        return self.response.json()  # type: ignore

    def get_links(self, rel: Optional[str] = None) -> List[Dict[str, str]]:
        links = []
        for link in self.json.get("links", []):
            if rel is not None and link.get("rel") == rel:
                links.append(link)
        return links

    def get_link_href(self, **kwargs: str) -> str:
        links = self.get_links(**kwargs)
        if len(links) != 1:
            raise RuntimeError(f"link not found or not unique {kwargs}")
        return links[0]["href"]


@attrs.define
class ProcessList(ApiResponse):
    def process_ids(self) -> List[str]:
        return [proc["id"] for proc in self.json["processes"]]


@attrs.define
class Process(ApiResponse):
    @property
    def id(self) -> str:
        process_id = self.json["id"]
        assert isinstance(process_id, str)
        return process_id

    def execute(self, inputs: Dict[str, Any], **kwargs: Any) -> StatusInfo:
        assert "json" not in kwargs
        url = f"{self.response.request.url}/execute"
        return StatusInfo.from_request("post", url, json={"inputs": inputs}, **kwargs)


@attrs.define(slots=False)
class Remote:
    url: str
    sleep_max: int = 120

    @functools.cached_property
    def request_uid(self) -> str:
        return self.url.rpartition("/")[2]

    @property
    def status(self) -> str:
        # TODO: cache responses for a timeout (possibly reported nby the server)
        requests_response = requests.get(self.url)
        requests_response.raise_for_status()
        json = requests_response.json()
        return json["status"]  # type: ignore

    def _robust_status(self, retry_options: Dict[str, Any] = {}) -> str:
        # TODO: cache responses for a timeout (possibly reported nby the server)
        requests_response = multiurl.robust(requests.get, **retry_options)(self.url)
        requests_response.raise_for_status()
        json = requests_response.json()
        return json["status"]  # type: ignore

    def wait_on_result(self, retry_options: Dict[str, Any] = {}) -> None:
        sleep = 1.0
        last_status = self._robust_status(retry_options=retry_options)
        while True:
            status = self._robust_status(retry_options=retry_options)
            if last_status != status:
                logger.debug(f"status has been updated to {status}")
            if status == "successful":
                break
            elif status == "failed":
                results = self._make_results_robust(retry_options=retry_options)
                info = results.json
                error_message = "processing failed"
                if info.get("title"):
                    error_message = f'{info["title"]}'
                if info.get("detail"):
                    error_message = error_message + f': {info["detail"]}'
                raise ProcessingFailedError(error_message)
                break
            elif status in ("accepted", "running"):
                sleep *= 1.5
                if sleep > self.sleep_max:
                    sleep = self.sleep_max
            else:
                raise ProcessingFailedError(f"Unknown API state {status!r}")
            logger.debug(f"result not ready, waiting for {sleep} seconds")
            time.sleep(sleep)

    def build_status_info(self) -> StatusInfo:
        return StatusInfo.from_request("get", self.url)

    def make_results(self) -> Results:
        if self.status not in ("successful", "failed"):
            raise Exception(f"Result not ready, job is {self.status}")
        request_response = requests.get(self.url)
        response = ApiResponse(request_response)
        try:
            results_url = response.get_link_href(rel="results")
        except RuntimeError:
            results_url = f"{self.url}/results"
        request_result = requests.get(results_url)
        results = Results(request_result)
        return results

    def _make_results_robust(
        self,
        retry_options: Dict[str, Any] = {},
    ) -> Results:
        if self.status not in ("successful", "failed"):
            raise Exception(f"Result not ready, job is {self.status}")
        request_response = multiurl.robust(
            requests.get,
            **retry_options,
        )(self.url)
        response = ApiResponse(request_response)
        try:
            results_url = response.get_link_href(rel="results")
        except RuntimeError:
            results_url = f"{self.url}/results"
        request_result = multiurl.robust(
            requests.get,
            **retry_options,
        )(results_url)
        results = Results(request_result)
        return results

    def _download_result(
        self, target: Optional[str] = None, retry_options: Dict[str, Any] = {}
    ) -> str:
        results = self._make_results_robust(retry_options=retry_options)
        return results.download(target, retry_options=retry_options)

    def download(
        self, target: Optional[str] = None, retry_options: Dict[str, Any] = {}
    ) -> str:
        self.wait_on_result(retry_options=retry_options)
        return self._download_result(target, retry_options=retry_options)


@attrs.define
class StatusInfo(ApiResponse):
    def make_remote(self) -> Remote:
        if self.response.request.method == "POST":
            url = self.get_link_href(rel="monitor")
        else:
            url = self.get_link_href(rel="self")
        return Remote(url)


@attrs.define
class JobList(ApiResponse):
    def job_ids(self) -> List[str]:
        return [job["jobID"] for job in self.json["jobs"]]


@attrs.define
class Results(ApiResponse):
    def get_result_href(self) -> Optional[str]:
        asset = self.json.get("asset", {}).get("value", {})
        result_href = asset.get("href")
        assert isinstance(result_href, str) or result_href is None
        return result_href

    def get_result_checksum(self) -> Optional[str]:
        asset = self.json.get("asset", {}).get("value", {})
        result_checksum = asset.get("file:checksum")
        assert isinstance(result_checksum, str) or result_checksum is None
        return result_checksum

    def get_result_size(self) -> Optional[int]:
        asset = self.json.get("asset", {}).get("value", {})
        size = asset["file:size"]
        return int(size)

    def download(
        self,
        target: Optional[str] = None,
        timeout: int = 60,
        retry_options: Dict[str, Any] = {},
    ) -> str:

        result_href = self.get_result_href()
        url = urllib.parse.urljoin(self.response.url, result_href)
        if target is None:
            parts = urllib.parse.urlparse(url)
            target = parts.path.strip("/").split("/")[-1]

        # FIXME add retry and progress bar
        retry_options = retry_options.copy()
        maximum_tries = retry_options.pop("maximum_tries", None)
        if maximum_tries is not None:
            retry_options["maximum_retries"] = maximum_tries
        multiurl.download(
            url, stream=True, target=target, timeout=timeout, **retry_options
        )
        target_size = os.path.getsize(target)
        size = self.get_result_size()
        if size:
            if target_size != size:
                raise DownloadError(
                    "Download failed: downloaded %s byte(s) out of %s"
                    % (target_size, size)
                )
        return target


class Processing:
    supported_api_version = "v1"

    def __init__(
        self, url: str, force_exact_url: bool = False, *args: Any, **kwargs: Any
    ) -> None:
        if not force_exact_url:
            url = f"{url}/{self.supported_api_version}"
        self.url = url

    def processes(self) -> ProcessList:
        url = f"{self.url}/processes"
        return ProcessList.from_request("get", url)

    def process(self, process_id: str) -> Process:
        url = f"{self.url}/processes/{process_id}"
        return Process.from_request("get", url)

    def process_execute(
        self, process_id: str, inputs: Dict[str, Any], **kwargs: Any
    ) -> StatusInfo:
        assert "json" not in kwargs
        url = f"{self.url}/processes/{process_id}/execute"
        return StatusInfo.from_request("post", url, json={"inputs": inputs}, **kwargs)

    def jobs(self) -> JobList:
        url = f"{self.url}/jobs"
        return JobList.from_request("get", url)

    def job(self, job_id: str) -> StatusInfo:
        url = f"{self.url}/jobs/{job_id}"
        return StatusInfo.from_request("get", url)

    def job_results(self, job_id: str) -> Results:
        url = f"{self.url}/jobs/{job_id}/results"
        return Results.from_request("get", url)

    # convenience methods

    def make_remote(self, job_id: str) -> Remote:
        url = f"{self.url}/jobs/{job_id}"
        return Remote(url)

    def download_result(
        self, job_id: str, target: Optional[str], retry_options: Dict[str, Any]
    ) -> str:
        # NOTE: the remote waits for the result to be available
        return self.make_remote(job_id).download(target, retry_options=retry_options)
