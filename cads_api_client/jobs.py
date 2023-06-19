
import functools
import logging
import os
import time
import urllib
from typing import Dict, Any, Optional, List

import attrs
import multiurl
from requests import Response

from cads_api_client.api_response import ApiResponse

# TODO add enum for status


logger = logging.getLogger(__name__)

def cond_cached(func):
    """
    Cache response for a remote job request only if 'status' field is equal to 'successful' or 'failed'.
    """
    @functools.wraps(func)
    def wrap(self, *args, **kwargs):
        name = func.__name__
        if not hasattr(self, name):
            values = func(self, *args, **kwargs)
            setattr(self, name, values)
        else:
            response = getattr(self, name)
            if response.json()["status"] not in ('successful', 'failed'):
                response = func(self, *args, **kwargs)
                setattr(self, name, response)
            response.raise_for_status()
        return getattr(self, name)

    return wrap


@attrs.define
class Job(ApiResponse):
    # note that this class extends the ResponseAPI class giving to it client capabilities.
    # we can perform new requests from the urls inside the response and get new responses.
    # we do that in order to monitor the status of the job.

    sleep_max = 120  # TODO pass as argument

    # TODO only one response (attribute for normal/robust)

    @functools.cached_property
    def id(self):
        return self.response.json().get("jobID")

    # @functools.lru_cache()
    def _get_monitor_href(self):
        url = self.get_link_href(rel="monitor")
        return url

    def monitor(self) -> Response:
        """
        Send request to get information about the remote job.
        """
        response = self.session.get(self._get_monitor_href(), headers=self.headers)
        response.raise_for_status()
        return response

    def _robust_monitor(self, retry_options: Dict[str, Any] = {}) -> Response:
        response = multiurl.robust(self.session.get, **retry_options)(self._get_monitor_href(), headers=self.headers)
        response.raise_for_status()
        return response

    @property
    def status(self) -> str:
        # note that this is the status of the job not of the job execution request
        return self.monitor().json().get("status")

    def _robust_status(self, retry_options: Dict[str, Any] = {}) -> str:
        return self._robust_monitor(retry_options=retry_options).json().get("status")

    def _make_results(self) -> ApiResponse:
        # if url is None:
        #     url = self.url
        if self.status not in ("successful", "failed"):
            raise Exception(f"Result not ready, job is {self.status}")
        url = self._get_monitor_href()
        request_response = self.session.get(url, headers=self.headers)
        response = ApiResponse(request_response, session=self.session)
        # request_response: ApiResponse = ApiResponse(self.response, session=self.session)
        try:
            results_url = response.get_link_href(rel="results")
        except RuntimeError:
            results_url = f"{url}/results"
        results = ApiResponse.from_request(
            "get",
            results_url,
            headers=self.headers,
            session=self.session,
            raise_for_status=False,
        )
        return results

    @property
    def results(self):
        return self._make_results()

    def _robust_results(self, retry_options: Dict[str, Any] = {}) -> str:
        return multiurl.robust(self._make_results, **retry_options)(self.url)  # TODO remove url

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
                results = multiurl.robust(self._make_results, **retry_options)(self.url)
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

    def _get_result_href(self) -> str:
        if self.results.response.status_code != 200:
            raise KeyError("result_href not available for processing failed results")
        href = self.json["asset"]["value"]["href"]
        assert isinstance(href, str)
        return href

    def _get_result_size(self) -> Optional[int]:
        asset = self.results.json.get("asset", {}).get("value", {})
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
        url = urllib.parse.urljoin(self._get_monitor_href(), self._get_result_href())

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
        size = self._get_result_size()
        if size:
            if target_size != size:
                raise DownloadError(
                    "Download failed: downloaded %s byte(s) out of %s"
                    % (target_size, size)
                )
        return target_path

    def _robust_download(
        self, target: Optional[str] = None, retry_options: Dict[str, Any] = {}
    ) -> str:   # TODO public (used for multiple requests)
        results = multiurl.robust(self._make_results, **retry_options)()
        return results.download(target, retry_options=retry_options)

@attrs.define
class JobList(ApiResponse):
    def job_ids(self) -> List[str]:
        return [job["jobID"] for job in self.json["jobs"]]

    def next(self) -> Optional[ApiResponse]:
        return self.from_rel_href(rel="next")

    def prev(self) -> Optional[ApiResponse]:
        return self.from_rel_href(rel="prev")


class ProcessingFailedError(RuntimeError):
    pass


class DownloadError(RuntimeError):
    pass
