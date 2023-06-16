from __future__ import annotations

import functools
import os
import time
import urllib
from typing import Dict, Any, Optional, List

import attrs
import multiurl
import requests

from cads_api_client.api_response import ApiResponse
from cads_api_client.processes import cond_cached, logger, ProcessingFailedError, DownloadError

# TODO add enum for status


class JobsAPIClient:
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

    # def job_results(self, job_id: str) -> Results:
    #     url = f"{self.url}/jobs/{job_id}/results"
    #     return Results.from_request(
    #         "get", url, headers=self.headers, session=self.session
    #     )

    @functools.cached_property
    def request_uid(self) -> str:
        return self.url.rpartition("/")[2]

    @property
    @cond_cached
    def response(self):
        response = self.session.get(self.url, headers=self.headers)
        response.raise_for_status()
        return response

    @functools.cached_property
    def id(self):
        return self.response.json()["jobID"]

    @property
    def status(self) -> str:
        # # TODO: cache responses for a timeout (possibly reported by the server)
        # requests_response = self.session.get(self.url, headers=self.headers)
        # requests_response.raise_for_status()
        # json = requests_response.json()
        # return json["status"]  # type: ignore
        return self.response.json()["status"]

    # @cond_cached
    # def _robust_response(self, retry_options: Dict[str, Any] = {}) -> str:
    #     return multiurl.robust(self.session.get, **retry_options)(self.url, headers=self.headers)

    def _robust_status(self, retry_options: Dict[str, Any] = {}) -> str:
        # TODO: cache responses for a timeout (possibly reported nby the server)
        requests_response = multiurl.robust(self.session.get, **retry_options)(
            self.url, headers=self.headers
        )
        requests_response.raise_for_status()
        json = requests_response.json()
        return json["status"]  # type: ignore
        # return self._robust_response(retry_options=retry_options).json()["status"]  # type: ignore

    def wait_on_result(self, retry_options: Dict[str, Any] = {}) -> None:
        """
        Poll periodically the current request for its status (accepted, running, successful, failed) until it has been
        processed (successful, failed). The wait time increases automatically from 1 second up to ``sleep_max``.

        Parameters
        ----------
        retry_options: retry options for the ``robust`` method in ``multiurl`` library.

        Returns
        -------

        """
        sleep = 1.0
        last_status = self._robust_status(retry_options=retry_options)
        while True:
            status = self._robust_status(retry_options=retry_options)
            if last_status != status:
                logger.debug(f"status has been updated to {status}")
            if status == "successful":
                break
            elif status == "failed":
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
        return status

    def make_results(self, url: Optional[str] = None) -> Job:  # TODO remove url
        if url is None:
            url = self.url
        if self.status not in ("successful", "failed"):
            raise Exception(f"Result not ready, job is {self.status}")
        # url = url if url else self.url
        request_response = self.session.get(url, headers=self.headers)
        response = ApiResponse(request_response, session=self.session)
        # request_response: ApiResponse = ApiResponse(self.response, session=self.session)
        try:
            results_url = response.get_link_href(rel="results")
        except RuntimeError:
            results_url = f"{url}/results"
        results = Job.from_request(
            "get",
            results_url,
            headers=self.headers,
            session=self.session,
            raise_for_status=False,
        )
        return results

    @property
    @cond_cached
    def results(self):
        return self.make_results()

    @cond_cached
    def _robust_results(self, retry_options: Dict[str, Any] = {}) -> str:
        return multiurl.robust(self.make_results, **retry_options)(self.url)  # TODO remove url

    def _download_result(
        self, target: Optional[str] = None, retry_options: Dict[str, Any] = {}
    ) -> str:   # TODO public (used for multiple requests)
        results: Job = multiurl.robust(self.make_results, **retry_options)(self.url)  # TODO remove url
        return results.download(target, retry_options=retry_options)

    def download(
        self, target: Optional[str] = None, retry_options: Dict[str, Any] = {}
    ) -> str:
        self.wait_on_result(retry_options=retry_options)
        return self._download_result(target, retry_options=retry_options)


@attrs.define
class Job(ApiResponse):
    # needed to usr this class in `multiurl.robust`
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
        target_folder: str = '.',
        target: Optional[str] = None,
        timeout: int = 60,
        retry_options: Dict[str, Any] = {},
    ) -> str:
        if not os.path.exists(target_folder):
            raise ValueError("Selected folder does not exists!")
        elif not os.path.isdir(target_folder):
            raise ValueError("A folder must be specified instead of file.")
        result_href = self.get_result_href()
        url = urllib.parse.urljoin(self.response.url, result_href)
        if target is None:
            parts = urllib.parse.urlparse(url)
            target = parts.path.strip("/").split("/")[-1]
        target_path = os.path.join(target_folder, target)
        # FIXME add retry and progress bar
        retry_options = retry_options.copy()
        maximum_tries = retry_options.pop("maximum_tries", None)
        if maximum_tries is not None:
            retry_options["maximum_retries"] = maximum_tries
        multiurl.download(
            url, stream=True, target=target_path, timeout=timeout, **retry_options
        )
        target_size = os.path.getsize(target_path)
        size = self.get_result_size()
        if size:
            if target_size != size:
                raise DownloadError(
                    "Download failed: downloaded %s byte(s) out of %s"
                    % (target_size, size)
                )
        return target_path


@attrs.define
class JobList(ApiResponse):
    def job_ids(self) -> List[str]:
        return [job["jobID"] for job in self.json["jobs"]]

    def next(self) -> Optional[ApiResponse]:
        return self.from_rel_href(rel="next")

    def prev(self) -> Optional[ApiResponse]:
        return self.from_rel_href(rel="prev")


# @attrs.define
# class StatusInfo(ApiResponse):
#     def make_remote(self) -> JobsAPIClient:
#         if self.response.request.method == "POST":
#             url = self.get_link_href(rel="monitor")
#         else:
#             url = self.get_link_href(rel="self")
#         return JobsAPIClient(url, headers=self.headers, session=self.session)
