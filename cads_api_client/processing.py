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

from . import config

T_ApiResponse = TypeVar("T_ApiResponse", bound="ApiResponse")

logger = logging.getLogger(__name__)


class ProcessingFailedError(RuntimeError):
    pass


class DownloadError(RuntimeError):
    pass


class LinkError(Exception):
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
    headers: dict[str, str]
    session: requests.Session
    retry_options: dict[str, Any]
    sleep_max: int
    cleanup: bool

    @classmethod
    def from_request(
        cls: Type[T_ApiResponse],
        method: str,
        url: str,
        headers: dict[str, str],
        session: requests.Session | None,
        retry_options: dict[str, Any],
        sleep_max: int,
        cleanup: bool,
        **kwargs: Any,
    ) -> T_ApiResponse:
        if session is None:
            session = requests.Session()
        robust_request = multiurl.robust(session.request, **retry_options)

        inputs = kwargs.get("json", {}).get("inputs", {})
        logger.debug(f"{method.upper()} {url} {inputs or ''}".strip())
        response = robust_request(method, url, headers=headers, **kwargs)
        logger.debug(f"REPLY {response.text}")

        cads_raise_for_status(response)

        self = cls(
            response,
            headers=headers,
            session=session,
            retry_options=retry_options,
            sleep_max=sleep_max,
            cleanup=cleanup,
        )
        self.log_messages()
        return self

    @property
    def json(self) -> dict[str, Any]:
        json: dict[str, Any] = self.response.json()
        return json

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

    def get_link_href(self, rel: str | None = None) -> str:
        links = self.get_links(rel)
        if len(links) != 1:
            raise LinkError(f"link not found or not unique {rel=}")
        return links[0]["href"]

    def from_rel_href(self, rel: str) -> Self | None:
        rels = self.get_links(rel=rel)
        assert len(rels) <= 1
        if len(rels) == 1:
            out = self.from_request(
                "get",
                rels[0]["href"],
                headers=self.headers,
                session=self.session,
                retry_options=self.retry_options,
                sleep_max=self.sleep_max,
                cleanup=self.cleanup,
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

    def execute(self, inputs: dict[str, Any]) -> StatusInfo:
        url = f"{self.response.request.url}/execute"
        return StatusInfo.from_request(
            "post",
            url,
            json={"inputs": inputs},
            headers=self.headers,
            session=self.session,
            retry_options=self.retry_options,
            sleep_max=self.sleep_max,
            cleanup=self.cleanup,
        )

    def valid_values(self, request: dict[str, Any] = {}) -> dict[str, Any]:
        url = f"{self.response.request.url}/constraints"
        response = ApiResponse.from_request(
            "post",
            url,
            json={"inputs": request},
            headers=self.headers,
            session=self.session,
            retry_options=self.retry_options,
            sleep_max=self.sleep_max,
            cleanup=self.cleanup,
        )
        response.response.raise_for_status()
        return response.json


@attrs.define(slots=False)
class Remote:
    url: str
    headers: dict[str, str]
    session: requests.Session
    retry_options: dict[str, Any]
    sleep_max: int = 120
    cleanup: bool = False

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

    @property
    def _reply(self) -> dict[str, Any]:
        params = {"log": True}
        if self.log_start_time:
            params["logStartTime"] = self.log_start_time

        api_response = ApiResponse.from_request(
            "get",
            url=self.url,
            headers=self.headers,
            session=self.session,
            retry_options=self.retry_options,
            sleep_max=self.sleep_max,
            cleanup=self.cleanup,
            params=params,
        )
        return api_response.json

    @property
    def status(self) -> str:
        reply = self._reply
        self.log_metadata(reply.get("metadata", {}))
        return str(reply["status"])

    def wait_on_result(self) -> None:
        sleep = 1.0
        status = None
        while True:
            if status != (status := self.status):
                self.info(f"status has been updated to {status}")
            if status == "successful":
                break
            elif status == "failed":
                results = self.make_results()
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
            "get",
            self.url,
            headers=self.headers,
            session=self.session,
            retry_options=self.retry_options,
            sleep_max=self.sleep_max,
            cleanup=self.cleanup,
        )

    def make_results(self) -> Results:
        response = ApiResponse.from_request(
            "GET",
            self.url,
            headers=self.headers,
            session=self.session,
            retry_options=self.retry_options,
            sleep_max=self.sleep_max,
            cleanup=self.cleanup,
        )
        try:
            results_url = response.get_link_href(rel="results")
        except LinkError:
            results_url = f"{self.url}/results"
        results = Results.from_request(
            "get",
            results_url,
            headers=self.headers,
            session=self.session,
            retry_options=self.retry_options,
            sleep_max=self.sleep_max,
            cleanup=self.cleanup,
        )
        return results

    def _download_result(
        self,
        target: str | None = None,
        timeout: int = 60,
    ) -> str:
        results: Results = self.make_results()
        return results.download(target, timeout=timeout)

    def download(
        self,
        target: str | None = None,
        timeout: int = 60,
    ) -> str:
        self.wait_on_result()
        return self._download_result(target, timeout=timeout)

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

        reply = self._reply
        reply.setdefault("state", reply["status"])

        if reply["state"] == "successful":
            reply["state"] = "completed"
        elif reply["state"] == "queued":
            reply["state"] = "accepted"
        elif reply["state"] == "failed":
            reply.setdefault("error", {})
            try:
                self.make_results()
            except Exception as exc:
                reply["error"].setdefault("message", str(exc))

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
    def make_remote(self) -> Remote:
        if self.response.request.method == "POST":
            url = self.get_link_href(rel="monitor")
        else:
            url = self.get_link_href(rel="self")
        return Remote(
            url,
            headers=self.headers,
            session=self.session,
            retry_options=self.retry_options,
            sleep_max=self.sleep_max,
            cleanup=self.cleanup,
        )


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
    ) -> str:
        url = self.location
        if target is None:
            parts = urllib.parse.urlparse(url)
            target = parts.path.strip("/").split("/")[-1]

        multiurl.download(
            url, stream=True, target=target, timeout=timeout, **self.retry_options
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
    headers: dict[str, str]
    session: requests.Session
    retry_options: dict[str, Any]
    sleep_max: int
    cleanup: bool
    force_exact_url: bool = False

    def __attrs_post_init__(self) -> None:
        if not self.force_exact_url:
            self.url += f"/{config.SUPPORTED_API_VERSION}"

    def processes(self, params: dict[str, Any] = {}) -> ProcessList:
        url = f"{self.url}/processes"
        return ProcessList.from_request(
            "get",
            url,
            params=params,
            headers=self.headers,
            session=self.session,
            retry_options=self.retry_options,
            sleep_max=self.sleep_max,
            cleanup=self.cleanup,
        )

    def process(self, process_id: str) -> Process:
        url = f"{self.url}/processes/{process_id}"
        return Process.from_request(
            "get",
            url,
            headers=self.headers,
            session=self.session,
            retry_options=self.retry_options,
            sleep_max=self.sleep_max,
            cleanup=self.cleanup,
        )

    def process_execute(
        self,
        process_id: str,
        inputs: dict[str, Any],
    ) -> StatusInfo:
        url = f"{self.url}/processes/{process_id}/execute"
        return StatusInfo.from_request(
            "post",
            url,
            json={"inputs": inputs},
            headers=self.headers,
            session=self.session,
            retry_options=self.retry_options,
            sleep_max=self.sleep_max,
            cleanup=self.cleanup,
        )

    def jobs(self, params: dict[str, Any] = {}) -> JobList:
        url = f"{self.url}/jobs"
        return JobList.from_request(
            "get",
            url,
            params=params,
            headers=self.headers,
            session=self.session,
            retry_options=self.retry_options,
            sleep_max=self.sleep_max,
            cleanup=self.cleanup,
        )

    def job(self, job_id: str) -> StatusInfo:
        url = f"{self.url}/jobs/{job_id}"
        return StatusInfo.from_request(
            "get",
            url,
            headers=self.headers,
            session=self.session,
            retry_options=self.retry_options,
            sleep_max=self.sleep_max,
            cleanup=self.cleanup,
        )

    def job_results(self, job_id: str) -> Results:
        url = f"{self.url}/jobs/{job_id}/results"
        return Results.from_request(
            "get",
            url,
            headers=self.headers,
            session=self.session,
            retry_options=self.retry_options,
            sleep_max=self.sleep_max,
            cleanup=self.cleanup,
        )

    # convenience methods

    def submit(self, collection_id: str, **request: Any) -> Remote:
        status_info = self.process_execute(collection_id, request)
        return status_info.make_remote()

    def submit_and_wait_on_result(self, collection_id: str, **request: Any) -> Results:
        remote = self.submit(collection_id, **request)
        remote.wait_on_result()
        return remote.make_results()

    def make_remote(self, job_id: str) -> Remote:
        url = f"{self.url}/jobs/{job_id}"
        return Remote(
            url,
            headers=self.headers,
            session=self.session,
            retry_options=self.retry_options,
            sleep_max=self.sleep_max,
            cleanup=self.cleanup,
        )

    def download_result(self, job_id: str, target: str | None) -> str:
        # NOTE: the remote waits for the result to be available
        return self.make_remote(job_id).download(target)
