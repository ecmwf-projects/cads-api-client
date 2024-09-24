from __future__ import annotations

import functools
import logging
import os
import time
import urllib.parse
import warnings
from typing import Any, Type, TypedDict, TypeVar

try:
    from typing import Self
except ImportError:
    from typing_extensions import Self

import attrs
import multiurl
import requests

import cads_api_client

from . import config

T_ApiResponse = TypeVar("T_ApiResponse", bound="ApiResponse")

logger = logging.getLogger(__name__)

LEVEL_NAMES_MAPPING = {
    "CRITICAL": 50,
    "FATAL": 50,
    "ERROR": 40,
    "WARNING": 30,
    "WARN": 30,
    "INFO": 20,
    "DEBUG": 10,
    "NOTSET": 0,
}


class RequestKwargs(TypedDict):
    headers: dict[str, str]
    session: requests.Session
    retry_options: dict[str, Any]
    request_options: dict[str, Any]
    download_options: dict[str, Any]
    sleep_max: float
    cleanup: bool


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
            raise requests.HTTPError(message, response=response)
    response.raise_for_status()


def get_level_and_message(message: str) -> tuple[int, str]:
    level = 20
    for severity in LEVEL_NAMES_MAPPING:
        if message.startswith(severity):
            level = LEVEL_NAMES_MAPPING[severity]
            message = message.replace(severity, "", 1).lstrip(":").lstrip()
            break
    return level, message


@attrs.define(slots=False)
class ApiResponse:
    response: requests.Response
    headers: dict[str, str]
    session: requests.Session
    retry_options: dict[str, Any]
    request_options: dict[str, Any]
    download_options: dict[str, Any]
    sleep_max: float
    cleanup: bool

    @property
    def _request_kwargs(self) -> RequestKwargs:
        return RequestKwargs(
            headers=self.headers,
            session=self.session,
            retry_options=self.retry_options,
            request_options=self.request_options,
            download_options=self.download_options,
            sleep_max=self.sleep_max,
            cleanup=self.cleanup,
        )

    @classmethod
    def from_request(
        cls: Type[T_ApiResponse],
        method: str,
        url: str,
        headers: dict[str, str],
        session: requests.Session | None,
        retry_options: dict[str, Any],
        request_options: dict[str, Any],
        download_options: dict[str, Any],
        sleep_max: float,
        cleanup: bool,
        **kwargs: Any,
    ) -> T_ApiResponse:
        if session is None:
            session = requests.Session()
        robust_request = multiurl.robust(session.request, **retry_options)

        inputs = kwargs.get("json", {}).get("inputs", {})
        logger.debug(f"{method.upper()} {url} {inputs or ''}".strip())
        response = robust_request(
            method, url, headers=headers, **request_options, **kwargs
        )
        logger.debug(f"REPLY {response.text}")

        cads_raise_for_status(response)

        self = cls(
            response,
            headers=headers,
            session=session,
            retry_options=retry_options,
            request_options=request_options,
            download_options=download_options,
            sleep_max=sleep_max,
            cleanup=cleanup,
        )
        self.log_messages()
        return self

    @property
    def url(self) -> str:
        """URL.

        Returns
        -------
        str
        """
        return str(self.response.request.url)

    @property
    def json(self) -> dict[str, Any]:
        """Content of the response.

        Returns
        -------
        dict[str,Any]
        """
        return dict(self.response.json())

    def log_messages(self) -> None:
        if message := self.json.get("message"):
            level, message = get_level_and_message(message)
            logger.log(level, message)

        dataset_messages = (
            self.json.get("metadata", {}).get("datasetMetadata", {}).get("messages", [])
        )
        for dataset_message in dataset_messages:
            if not (content := dataset_message.get("content")):
                continue
            if date := dataset_message.get("date"):
                content = f"[{date}] {content}"
            severity = dataset_message.get("severity", "notset").upper()
            level = LEVEL_NAMES_MAPPING.get(severity, 20)
            logger.log(level, content)

    def _get_links(self, rel: str | None = None) -> list[dict[str, str]]:
        links = []
        for link in self.json.get("links", []):
            if rel is not None and link.get("rel") == rel:
                links.append(link)
        return links

    def _get_link_href(self, rel: str | None = None) -> str:
        links = self._get_links(rel)
        if len(links) != 1:
            raise LinkError(f"link not found or not unique {rel=}")
        return links[0]["href"]

    def _from_rel_href(self, rel: str) -> Self | None:
        rels = self._get_links(rel=rel)
        if len(rels) > 1:
            raise LinkError(f"link not unique {rel=}")

        if len(rels) == 1:
            out = self.from_request("get", rels[0]["href"], **self._request_kwargs)
        else:
            out = None
        return out


@attrs.define
class ApiResponsePaginated(ApiResponse):
    @property
    def next(self) -> Self | None:
        """Next page.

        Returns
        -------
        Self or None
        """
        return self._from_rel_href(rel="next")

    @property
    def prev(self) -> Self | None:
        """Previous page.

        Returns
        -------
        Self or None
        """
        return self._from_rel_href(rel="prev")


@attrs.define
class Processes(ApiResponsePaginated):
    """A class to interact with available processes."""

    @property
    def process_ids(self) -> list[str]:
        """Available process IDs.

        Returns
        -------
        list[str]
        """
        return [proc["id"] for proc in self.json["processes"]]


@attrs.define
class Process(ApiResponse):
    """A class to interact with a process."""

    @property
    def id(self) -> str:
        """Process ID.

        Returns
        -------
        str
        """
        process_id: str = self.json["id"]
        return process_id

    def submit(self, **request: Any) -> cads_api_client.Remote:
        """Submit a request.

        Parameters
        ----------
        **request: Any
            Request parameters.

        Returns
        -------
        cads_api_client.Remote
        """
        job = Job.from_request(
            "post",
            f"{self.url}/execution",
            json={"inputs": request},
            **self._request_kwargs,
        )
        return job.make_remote()

    def apply_constraints(self, **request: Any) -> dict[str, Any]:
        """Apply constraints to the parameters in a request.

        Parameters
        ----------
        **request: Any
            Request parameters.

        Returns
        -------
        dict[str,Any]
            Dictionary of valid values.
        """
        response = ApiResponse.from_request(
            "post",
            f"{self.url}/constraints",
            json={"inputs": request},
            **self._request_kwargs,
        )
        return response.json

    def estimate_costs(self, **request: Any) -> dict[str, Any]:
        """Estimate costs of the parameters in a request.

        Parameters
        ----------
        **request: Any
            Request parameters.

        Returns
        -------
        dict[str,Any]
            Dictionary of estimated costs.
        """
        response = ApiResponse.from_request(
            "post",
            f"{self.url}/costing",
            json={"inputs": request},
            **self._request_kwargs,
        )
        return response.json


@attrs.define(slots=False)
class Remote:
    """A class to interact with a submitted job."""

    url: str
    headers: dict[str, str]
    session: requests.Session
    retry_options: dict[str, Any]
    request_options: dict[str, Any]
    download_options: dict[str, Any]
    sleep_max: float
    cleanup: bool

    def __attrs_post_init__(self) -> None:
        self.log_start_time = None
        self.info(f"Request ID is {self.request_uid}")

    @property
    def _request_kwargs(self) -> RequestKwargs:
        return RequestKwargs(
            headers=self.headers,
            session=self.session,
            retry_options=self.retry_options,
            request_options=self.request_options,
            download_options=self.download_options,
            sleep_max=self.sleep_max,
            cleanup=self.cleanup,
        )

    def _log_metadata(self, metadata: dict[str, Any]) -> None:
        logs = metadata.get("log", [])
        for self.log_start_time, message in sorted(logs):
            level, message = get_level_and_message(message)
            logger.log(level, message)

    def _get_api_response(self, method: str, **kwargs: Any) -> ApiResponse:
        return ApiResponse.from_request(
            method, self.url, **self._request_kwargs, **kwargs
        )

    @property
    def request_uid(self) -> str:
        """Request UID.

        Returns
        -------
        str
        """
        return self.url.rpartition("/")[2]

    @property
    def json(self) -> dict[str, Any]:
        """Content of the response.

        Returns
        -------
        dict[str,Any]
        """
        params = {"log": True, "request": True}
        if self.log_start_time:
            params["logStartTime"] = self.log_start_time
        return self._get_api_response("get", params=params).json

    @property
    def collection_id(self) -> str:
        """Collection ID.

        Returns
        -------
        str
        """
        return str(self.json["processID"])

    @property
    def request(self) -> dict[str, Any]:
        """Request parameters.

        Returns
        -------
        dict[str,Any]
        """
        return dict(self.json["metadata"]["request"]["ids"])

    @property
    def status(self) -> str:
        """Request status.

        Returns
        -------
        str
        """
        reply = self.json
        self._log_metadata(reply.get("metadata", {}))
        status: str = reply["status"]
        return status

    def _wait_on_results(self) -> None:
        sleep = 1.0
        status = None
        while True:
            if status != (status := self.status):
                self.info(f"status has been updated to {status}")
            if status == "successful":
                break
            elif status == "failed":
                results = self.make_results(wait=False)
                raise ProcessingFailedError(error_json_to_message(results.json))
            elif status in ("accepted", "running"):
                sleep = min(sleep * 1.5, self.sleep_max)
            elif status in ("dismissed", "deleted"):
                raise ProcessingFailedError(f"API state {status!r}")
            else:
                raise ProcessingFailedError(f"Unknown API state {status!r}")
            self.debug(f"results not ready, waiting for {sleep} seconds")
            time.sleep(sleep)

    def make_results(self, wait: bool = True) -> Results:
        if wait:
            self._wait_on_results()
        response = self._get_api_response("get")
        try:
            results_url = response._get_link_href(rel="results")
        except LinkError:
            results_url = f"{self.url}/results"
        results = Results.from_request("get", results_url, **self._request_kwargs)
        return results

    def download(self, target: str | None = None) -> str:
        """Download the results.

        Parameters
        ----------
        target: str or None
            Target path. If None, download to the working directory.

        Returns
        -------
        str
            Path to the retrieved file.
        """
        results = self.make_results()
        return results.download(target)

    def delete(self) -> dict[str, Any]:
        """Delete job.

        Returns
        -------
        dict[str,Any]
            Content of the response.
        """
        response = self._get_api_response("delete")
        self.cleanup = False
        return response.json

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

        reply = dict(self.json)
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
class Job(ApiResponse):
    def make_remote(self) -> Remote:
        if self.response.request.method == "POST":
            url = self._get_link_href(rel="monitor")
        else:
            url = self._get_link_href(rel="self")
        return Remote(url, **self._request_kwargs)


@attrs.define
class Jobs(ApiResponsePaginated):
    """A class to interact with submitted jobs."""

    @property
    def job_ids(self) -> list[str]:
        """List of job IDs.

        Returns
        -------
        list[str]
        """
        return [job["jobID"] for job in self.json["jobs"]]


@attrs.define
class Results(ApiResponse):
    """A class to interact with the results of a job."""

    def _check_size(self, target: str) -> None:
        if (target_size := os.path.getsize(target)) != (size := self.content_length):
            raise DownloadError(
                f"Download failed: downloaded {target_size} byte(s) out of {size}"
            )

    @property
    def asset(self) -> dict[str, Any]:
        """Asset dictionary.

        Returns
        -------
        dict[str,Any]
        """
        return dict(self.json["asset"]["value"])

    def download(
        self,
        target: str | None = None,
    ) -> str:
        """Download the results.

        Parameters
        ----------
        target: str or None
            Target path. If None, download to the working directory.

        Returns
        -------
        str
            Path to the retrieved file.
        """
        url = self.location
        if target is None:
            parts = urllib.parse.urlparse(url)
            target = parts.path.strip("/").split("/")[-1]

        download_options = {"stream": True}
        download_options.update(self.download_options)
        multiurl.download(
            url,
            target=target,
            **self.retry_options,
            **self.request_options,
            **download_options,
        )
        self._check_size(target)
        return target

    # cdsapi backward compatibility methods
    @property
    def location(self) -> str:
        result_href = self.asset["href"]
        return urllib.parse.urljoin(self.response.url, result_href)

    @property
    def content_length(self) -> int:
        return int(self.asset["file:size"])

    @property
    def content_type(self) -> str:
        return str(self.asset["type"])

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
    request_options: dict[str, Any]
    download_options: dict[str, Any]
    sleep_max: float
    cleanup: bool
    force_exact_url: bool = False

    def __attrs_post_init__(self) -> None:
        if not self.force_exact_url:
            self.url += f"/{config.SUPPORTED_API_VERSION}"

    @property
    def _request_kwargs(self) -> RequestKwargs:
        return RequestKwargs(
            headers=self.headers,
            session=self.session,
            retry_options=self.retry_options,
            request_options=self.request_options,
            download_options=self.download_options,
            sleep_max=self.sleep_max,
            cleanup=self.cleanup,
        )

    def get_processes(self, **params: Any) -> Processes:
        url = f"{self.url}/processes"
        return Processes.from_request("get", url, params=params, **self._request_kwargs)

    def get_process(self, process_id: str) -> Process:
        url = f"{self.url}/processes/{process_id}"
        return Process.from_request("get", url, **self._request_kwargs)

    def get_jobs(self, **params: Any) -> Jobs:
        url = f"{self.url}/jobs"
        return Jobs.from_request("get", url, params=params, **self._request_kwargs)

    def get_job(self, job_id: str) -> Job:
        url = f"{self.url}/jobs/{job_id}"
        return Job.from_request("get", url, **self._request_kwargs)

    def submit(self, collection_id: str, **request: Any) -> Remote:
        return self.get_process(collection_id).submit(**request)
