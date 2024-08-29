from __future__ import annotations

import functools
import logging
import os
import time
import urllib.parse
import warnings
from typing import Any, Type, TypeVar

try:
    from typing import Self
except ImportError:
    from typing_extensions import Self

import attrs
import multiurl
import requests

T_ApiResponse = TypeVar("T_ApiResponse", bound="ApiResponse")

logger = logging.getLogger(__name__)


class ProcessingFailedError(RuntimeError):
    pass


class DownloadError(RuntimeError):
    pass


def error_json_to_message(error_json: dict[str, Any]) -> str:
    error_messages = [
        str(error_json[key])
        for key in ("title", "traceback", "detail")
        if key in error_json
    ]
    return "\n".join(error_messages)


def cads_raise_for_status(response: requests.Response) -> None:
    if 400 <= response.status_code < 500:
        try:
            error_json = response.json()
        except Exception:
            pass
        else:
            message = "\n".join(
                [
                    f"{response.status_code} Client Error: {response.reason} for url: {response.url}",
                    error_json_to_message(error_json),
                ]
            )
            raise requests.exceptions.HTTPError(message, response=response)
    response.raise_for_status()


@attrs.define(slots=False)
class ApiResponse:
    response: requests.Response
    headers: dict[str, Any] = {}
    session: requests.Session = attrs.field(factory=requests.Session)

    @classmethod
    def from_request(
        cls: Type[T_ApiResponse],
        *args: Any,
        raise_for_status: bool = True,
        session: requests.Session | None = None,
        retry_options: dict[str, Any] = {"maximum_tries": 2, "retry_after": 10},
        **kwargs: Any,
    ) -> T_ApiResponse:
        if session is None:
            session = requests.Session()
        method = kwargs["method"] if "method" in kwargs else args[0]
        url = kwargs["url"] if "url" in kwargs else args[1]
        inputs = kwargs.get("json", {}).get("inputs", {})
        logger.debug(f"{method.upper()} {url} {inputs}")
        response = multiurl.robust(session.request, **retry_options)(*args, **kwargs)
        logger.debug(f"REPLY {response.text}")

        if raise_for_status:
            cads_raise_for_status(response)
        self = cls(response, headers=kwargs.get("headers", {}), session=session)
        self.log_messages()
        return self

    @functools.cached_property
    def json(self) -> dict[str, Any]:
        return dict(self.response.json())

    def log_messages(self) -> None:
        messages = (
            self.json.get("metadata", {}).get("datasetMetadata", {}).get("messages", [])
        )
        for message in messages:
            if not (content := message.get("content")):
                continue
            if date := message.get("date"):
                content = f"[{date}] {content}"
            severity = message.get("severity", "notset").upper()
            level = logging.getLevelName(severity)
            logger.log(level if isinstance(level, int) else 20, content)

    def get_links(self, rel: str | None = None) -> list[dict[str, str]]:
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

    def from_rel_href(self, rel: str) -> Self | None:
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
    def process_ids(self) -> list[str]:
        return [proc["id"] for proc in self.json["processes"]]

    def next(self) -> ApiResponse | None:
        return self.from_rel_href(rel="next")

    def prev(self) -> ApiResponse | None:
        return self.from_rel_href(rel="prev")


@attrs.define
class Process(ApiResponse):
    @property
    def id(self) -> str:
        process_id = self.json["id"]
        assert isinstance(process_id, str)
        return process_id

    def execute(
        self,
        inputs: dict[str, Any],
        retry_options: dict[str, Any] = {},
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

    def valid_values(self, request: dict[str, Any] = {}) -> dict[str, Any]:
        url = f"{self.response.request.url}/constraints"
        response = ApiResponse.from_request("post", url, json={"inputs": request})
        response.response.raise_for_status()
        return response.json


@attrs.define(slots=False)
class Remote:
    url: str
    headers: dict[str, Any] = {}
    sleep_max: int = 120
    cleanup: bool = False
    session: requests.Session = attrs.field(factory=requests.Session)

    def __attrs_post_init__(self) -> None:
        self.log_start_time = None
        self.info(f"Request ID is {self.request_uid}")

    def log_metadata(self, metadata: dict[str, Any]) -> None:
        logs = metadata.get("log", [])
        for self.log_start_time, message in sorted(logs):
            level = 20
            for severity in ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"):
                if message.startswith(severity):
                    level = logging.getLevelName(severity)
                    message = message.replace(severity, "", 1).lstrip(":").lstrip()
                    break
            logger.log(level, message)

    @functools.cached_property
    def request_uid(self) -> str:
        return self.url.rpartition("/")[2]

    def _get_reply(self, robust: bool, **retry_options: Any) -> dict[str, Any]:
        # TODO: cache responses for a timeout (possibly reported nby the server)
        get = self.session.get
        if robust:
            get = multiurl.robust(get, **retry_options)

        params = {"log": True}
        if self.log_start_time:
            params["logStartTime"] = self.log_start_time

        self.debug(f"GET {self.url}")
        requests_response = get(url=self.url, headers=self.headers, params=params)
        self.debug(f"REPLY {requests_response.text}")
        requests_response.raise_for_status()
        return dict(requests_response.json())

    def _get_status(self, robust: bool, **retry_options: Any) -> str:
        json = self._get_reply(robust, **retry_options)
        self.log_metadata(json.get("metadata", {}))
        return str(json["status"])

    @property
    def status(self) -> str:
        return self._get_status(robust=False)

    def _robust_status(self, retry_options: dict[str, Any] = {}) -> str:
        return self._get_status(robust=True, **retry_options)

    def wait_on_result(self, retry_options: dict[str, Any] = {}) -> None:
        sleep = 1.0
        status = None
        while True:
            if status != (status := self._robust_status(retry_options=retry_options)):
                self.info(f"status has been updated to {status}")
            if status == "successful":
                break
            elif status == "failed":
                results = multiurl.robust(self.make_results, **retry_options)(self.url)
                raise ProcessingFailedError(error_json_to_message(results.json))
            elif status in ("accepted", "running"):
                sleep *= 1.5
                if sleep > self.sleep_max:
                    sleep = self.sleep_max
            elif status == "dismissed":
                raise ProcessingFailedError(f"API state {status!r}")
            else:
                raise ProcessingFailedError(f"Unknown API state {status!r}")
            self.debug(f"result not ready, waiting for {sleep} seconds")
            time.sleep(sleep)

    def build_status_info(self) -> StatusInfo:
        return StatusInfo.from_request(
            "get", self.url, headers=self.headers, session=self.session
        )

    def make_results(self, url: str | None = None) -> Results:
        if url is None:
            url = self.url

        self.debug(f"GET {url}")
        request_response = self.session.get(url, headers=self.headers)
        self.debug(f"REPLY {request_response.text}")

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
            raise_for_status=True,
        )
        return results

    def _download_result(
        self,
        target: str | None = None,
        timeout: int = 60,
        retry_options: dict[str, Any] = {},
    ) -> str:
        results: Results = multiurl.robust(self.make_results, **retry_options)(self.url)
        return results.download(target, timeout=timeout, retry_options=retry_options)

    def download(
        self,
        target: str | None = None,
        timeout: int = 60,
        retry_options: dict[str, Any] = {},
    ) -> str:
        self.wait_on_result(retry_options=retry_options)
        return self._download_result(
            target, timeout=timeout, retry_options=retry_options
        )

    def delete(self) -> dict[str, Any]:
        self.debug(f"DELETE {self.url}")
        requests_response = self.session.delete(url=self.url, headers=self.headers)
        self.debug(f"REPLY {requests_response.text}")
        requests_response.raise_for_status()
        self.cleanup = False
        return dict(requests_response.json())

    def _warn(self) -> None:
        message = (
            ".update and .reply are available for backward compatibility."
            " You can now use .download directly without needing to check whether the request is completed."
        )
        warnings.warn(message, DeprecationWarning)

    def update(self, request_id: str | None = None) -> None:
        self._warn()
        if request_id:
            assert request_id == self.request_uid
        try:
            del self.reply
        except AttributeError:
            pass
        self.reply

    @functools.cached_property
    def reply(self) -> dict[str, Any]:
        self._warn()

        reply = self._get_reply(True)

        reply.setdefault("state", reply["status"])
        if reply["state"] == "successful":
            reply["state"] = "completed"
        elif reply["state"] == "queued":
            reply["state"] = "accepted"
        elif reply["state"] == "failed":
            results = multiurl.robust(self.make_results)(self.url)
            message = error_json_to_message(results.json)
            reply.setdefault("error", {})
            reply["error"].setdefault("message", message)

        reply.setdefault("request_id", self.request_uid)
        return reply

    def info(self, *args: Any, **kwargs: Any) -> None:
        logger.info(*args, **kwargs)

    def warning(self, *args: Any, **kwargs: Any) -> None:
        logger.warning(*args, **kwargs)

    def error(self, *args: Any, **kwargs: Any) -> None:
        logger.error(*args, **kwargs)

    def debug(self, *args: Any, **kwargs: Any) -> None:
        logger.debug(*args, **kwargs)

    def __del__(self) -> None:
        if self.cleanup:
            try:
                self.delete()
            except Exception as exc:
                warnings.warn(str(exc), UserWarning)


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
    def job_ids(self) -> list[str]:
        return [job["jobID"] for job in self.json["jobs"]]

    def next(self) -> ApiResponse | None:
        return self.from_rel_href(rel="next")

    def prev(self) -> ApiResponse | None:
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

    @property
    def asset(self) -> dict[str, Any]:
        return dict(self.json["asset"]["value"])

    @property
    def location(self) -> str:
        result_href = self.get_result_href()
        return urllib.parse.urljoin(self.response.url, result_href)

    @property
    def content_length(self) -> int:
        return int(self.asset["file:size"])

    @property
    def content_type(self) -> str:
        return str(self.asset["type"])

    def download(
        self,
        target: str | None = None,
        timeout: int = 60,
        retry_options: dict[str, Any] = {},
    ) -> str:
        url = self.location
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
        if (target_size := os.path.getsize(target)) != (size := self.content_length):
            raise DownloadError(
                "Download failed: downloaded %s byte(s) out of %s" % (target_size, size)
            )
        return target

    def info(self, *args: Any, **kwargs: Any) -> None:
        logger.info(*args, **kwargs)

    def warning(self, *args: Any, **kwargs: Any) -> None:
        logger.warning(*args, **kwargs)

    def error(self, *args: Any, **kwargs: Any) -> None:
        logger.error(*args, **kwargs)

    def debug(self, *args: Any, **kwargs: Any) -> None:
        logger.debug(*args, **kwargs)


@attrs.define(slots=False)
class Processing:
    url: str
    force_exact_url: bool = False
    headers: dict[str, Any] = {}
    sleep_max: int = 120
    cleanup: bool = False
    session: requests.Session = attrs.field(factory=requests.Session)
    supported_api_version: str = "v1"

    def __attrs_post_init__(self) -> None:
        if not self.force_exact_url:
            self.url += f"/{self.supported_api_version}"

    def processes(self, params: dict[str, Any] = {}) -> ProcessList:
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
        inputs: dict[str, Any],
        retry_options: dict[str, Any] = {},
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

    def jobs(self, params: dict[str, Any] = {}) -> JobList:
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
        self, collection_id: str, retry_options: dict[str, Any] = {}, **request: Any
    ) -> Remote:
        status_info = self.process_execute(
            collection_id, request, retry_options=retry_options
        )
        return status_info.make_remote(sleep_max=self.sleep_max, cleanup=self.cleanup)

    def submit_and_wait_on_result(
        self, collection_id: str, retry_options: dict[str, Any] = {}, **request: Any
    ) -> Results:
        remote = self.submit(collection_id, retry_options=retry_options, **request)
        remote.wait_on_result(retry_options=retry_options)
        return remote.make_results()

    def make_remote(self, job_id: str) -> Remote:
        url = f"{self.url}/jobs/{job_id}"
        return Remote(
            url,
            headers=self.headers,
            session=self.session,
            sleep_max=self.sleep_max,
            cleanup=self.cleanup,
        )

    def download_result(
        self, job_id: str, target: str | None, retry_options: dict[str, Any]
    ) -> str:
        # NOTE: the remote waits for the result to be available
        return self.make_remote(job_id).download(target, retry_options=retry_options)
