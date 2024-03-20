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

logger = logging.getLogger(__name__)


class ProcessingFailedError(RuntimeError):
    pass


class DownloadError(RuntimeError):
    pass


def cads_raise_for_status(response: requests.Response) -> None:
    if response.status_code > 499:
        response.raise_for_status()
    if response.status_code > 399:
        error_json = None
        try:
            error_json = response.json()
        except Exception:
            pass
        if error_json is not None:
            raise RuntimeError(f"{response.status_code} Client Error: {error_json}")
        else:
            response.raise_for_status()


@attrs.define(slots=False)
class ApiResponse:
    response: requests.Response
    headers: Dict[str, Any] = {}
    session: requests.Session = (requests.api,)  # type: ignore

    @classmethod
    def from_request(
        cls: Type[T_ApiResponse],
        *args: Any,
        raise_for_status: bool = True,
        session: requests.Session = requests.api,  # type: ignore
        retry_options: Dict[str, Any] = {"maximum_tries": 2, "retry_after": 10},
        **kwargs: Any,
    ) -> T_ApiResponse:
        response = multiurl.robust(session.request, **retry_options)(*args, **kwargs)
        if raise_for_status:
            cads_raise_for_status(response)
        self = cls(response, headers=kwargs.get("headers", {}), session=session)
        self.log_messages()
        return self

    @functools.cached_property
    def json(self) -> Dict[str, Any]:
        return self.response.json()  # type: ignore

    def log_messages(self) -> None:
        messages = (
            self.json.get("metadata", {}).get("datasetMetadata", {}).get("messages", [])
        )
        for message in messages:
            if not (content := message.get("content")):
                continue
            severity = message.get("severity", "notset").upper()
            level = logging.getLevelName(severity)
            logger.log(level if isinstance(level, int) else 20, content)

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

    def from_rel_href(self, rel: str) -> Optional[ApiResponse]:
        rels = self.get_links(rel=rel)
        assert len(rels) <= 1
        if len(rels) == 1:
            out = self.from_request(
                "get", rels[0]["href"], headers=self.headers, session=self.session
            )
        else:
            out = None
        return out


@attrs.define
class ProcessList(ApiResponse):
    def process_ids(self) -> List[str]:
        return [proc["id"] for proc in self.json["processes"]]

    def next(self) -> Optional[ApiResponse]:
        return self.from_rel_href(rel="next")

    def prev(self) -> Optional[ApiResponse]:
        return self.from_rel_href(rel="prev")


@attrs.define
class Process(ApiResponse):
    headers: Dict[str, Any] = {}

    @property
    def id(self) -> str:
        process_id = self.json["id"]
        assert isinstance(process_id, str)
        return process_id

    def execute(
        self,
        inputs: Dict[str, Any],
        retry_options: Dict[str, Any] = {},
        **kwargs: Any,
    ) -> StatusInfo:
        assert "json" not in kwargs
        url = f"{self.response.request.url}/execute"
        json = {"inputs": inputs}
        return StatusInfo.from_request(
            "post",
            url,
            json=json,
            headers=self.headers,
            retry_options=retry_options,
            **kwargs,
        )

    def valid_values(self, request: Dict[str, Any] = {}) -> Dict[str, Any]:
        url = f"{self.response.request.url}/constraints"
        response = ApiResponse.from_request("post", url, json={"inputs": request})
        response.response.raise_for_status()
        return response.json


class Remote:
    def __init__(
        self,
        url: str,
        sleep_max: int = 120,
        headers: Dict[str, Any] = {},
        session: requests.Session = requests.api,  # type: ignore
    ):
        self.url = url
        self.sleep_max = sleep_max
        self.headers = headers
        self.session = session

    @functools.cached_property
    def request_uid(self) -> str:
        return self.url.rpartition("/")[2]

    @property
    def status(self) -> str:
        # TODO: cache responses for a timeout (possibly reported nby the server)
        requests_response = self.session.get(self.url, headers=self.headers)
        requests_response.raise_for_status()
        json = requests_response.json()
        return json["status"]  # type: ignore

    def _robust_status(self, retry_options: Dict[str, Any] = {}) -> str:
        # TODO: cache responses for a timeout (possibly reported nby the server)
        requests_response = multiurl.robust(self.session.get, **retry_options)(
            self.url, headers=self.headers
        )
        requests_response.raise_for_status()
        json = requests_response.json()
        return json["status"]  # type: ignore

    def wait_on_result(self, retry_options: Dict[str, Any] = {}) -> None:
        sleep = 1.0
        status = None
        while True:
            if status != (status := self._robust_status(retry_options=retry_options)):
                logger.info(f"status has been updated to {status}")
            if status == "successful":
                # workaround for the server-side 404 due to database replicas out od sync
                time.sleep(1)
                break
            elif status == "failed":
                # workaround for the server-side 404 due to database replicas out od sync
                time.sleep(1)
                results = multiurl.robust(self.make_results, **retry_options)(self.url)
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
        return StatusInfo.from_request(
            "get", self.url, headers=self.headers, session=self.session
        )

    def make_results(self, url: Optional[str] = None) -> Results:
        if url is None:
            url = self.url
        status = self.status
        if status not in ("successful", "failed"):
            raise ValueError(f"Result not ready, job is {status}")
        request_response = self.session.get(url, headers=self.headers)
        response = ApiResponse(request_response, session=self.session)
        try:
            results_url = response.get_link_href(rel="results")
        except RuntimeError:
            results_url = f"{url}/results"
        results = Results.from_request(
            "get",
            results_url,
            headers=self.headers,
            session=self.session,
            raise_for_status=False,
        )
        return results

    def _download_result(
        self, target: Optional[str] = None, retry_options: Dict[str, Any] = {}
    ) -> str:
        results: Results = multiurl.robust(self.make_results, **retry_options)(self.url)
        return results.download(target, retry_options=retry_options)

    def download(
        self, target: Optional[str] = None, retry_options: Dict[str, Any] = {}
    ) -> str:
        self.wait_on_result(retry_options=retry_options)
        return self._download_result(target, retry_options=retry_options)


@attrs.define
class StatusInfo(ApiResponse):
    def make_remote(self, **kwargs: Any) -> Remote:
        if self.response.request.method == "POST":
            url = self.get_link_href(rel="monitor")
        else:
            url = self.get_link_href(rel="self")
        return Remote(url, headers=self.headers, session=self.session, **kwargs)


@attrs.define
class JobList(ApiResponse):
    def job_ids(self) -> List[str]:
        return [job["jobID"] for job in self.json["jobs"]]

    def next(self) -> Optional[ApiResponse]:
        return self.from_rel_href(rel="next")

    def prev(self) -> Optional[ApiResponse]:
        return self.from_rel_href(rel="prev")


@attrs.define
class Results(ApiResponse):
    # needed to use this class in `multiurl.robust`
    @property
    def status_code(self) -> int:
        return self.response.status_code

    # needed to usr this class in `multiurl.robust`
    @property
    def reason(self) -> str:
        return self.response.reason

    def get_result_href(self) -> str:
        if self.status_code != 200:
            raise KeyError("result_href not available for processing failed results")
        href = self.json["asset"]["value"]["href"]
        assert isinstance(href, str)
        return href

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
        self,
        url: str,
        force_exact_url: bool = False,
        headers: Dict[str, Any] = {},
        session: requests.Session = requests.api,  # type: ignore
        sleep_max: int = 120,
    ) -> None:
        if not force_exact_url:
            url = f"{url}/{self.supported_api_version}"
        self.url = url
        self.headers = headers
        self.session = session
        self.sleep_max = sleep_max

    def processes(self, params: Dict[str, Any] = {}) -> ProcessList:
        url = f"{self.url}/processes"
        return ProcessList.from_request("get", url, params=params, session=self.session)

    def process(self, process_id: str) -> Process:
        url = f"{self.url}/processes/{process_id}"
        return Process.from_request(
            "get", url, headers=self.headers, session=self.session
        )

    def process_execute(
        self,
        process_id: str,
        inputs: Dict[str, Any],
        retry_options: Dict[str, Any] = {},
        **kwargs: Any,
    ) -> StatusInfo:
        assert "json" not in kwargs
        url = f"{self.url}/processes/{process_id}/execute"
        headers = kwargs.pop("headers", {})
        return StatusInfo.from_request(
            "post",
            url,
            json={"inputs": inputs},
            headers={**self.headers, **headers},
            session=self.session,
            retry_options=retry_options,
            **kwargs,
        )

    def jobs(self, params: Dict[str, Any] = {}) -> JobList:
        url = f"{self.url}/jobs"
        return JobList.from_request(
            "get", url, params=params, headers=self.headers, session=self.session
        )

    def job(self, job_id: str) -> StatusInfo:
        url = f"{self.url}/jobs/{job_id}"
        return StatusInfo.from_request(
            "get", url, headers=self.headers, session=self.session
        )

    def job_results(self, job_id: str) -> Results:
        url = f"{self.url}/jobs/{job_id}/results"
        return Results.from_request(
            "get", url, headers=self.headers, session=self.session
        )

    # convenience methods

    def submit(
        self, collection_id: str, retry_options: Dict[str, Any] = {}, **request: Any
    ) -> Remote:
        status_info = self.process_execute(
            collection_id, request, retry_options=retry_options
        )
        return status_info.make_remote(sleep_max=self.sleep_max)

    def submit_and_wait_on_result(
        self, collection_id: str, retry_options: Dict[str, Any] = {}, **request: Any
    ) -> Results:
        remote = self.submit(collection_id, retry_options=retry_options, **request)
        remote.wait_on_result(retry_options=retry_options)
        return remote.make_results()

    def make_remote(self, job_id: str) -> Remote:
        url = f"{self.url}/jobs/{job_id}"
        return Remote(
            url, headers=self.headers, session=self.session, sleep_max=self.sleep_max
        )

    def download_result(
        self, job_id: str, target: Optional[str], retry_options: Dict[str, Any]
    ) -> str:
        # NOTE: the remote waits for the result to be available
        return self.make_remote(job_id).download(target, retry_options=retry_options)
