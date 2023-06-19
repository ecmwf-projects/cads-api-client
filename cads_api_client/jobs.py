
import functools
import logging
import os
import time
import urllib
from typing import Dict, Any, Optional

import multiurl
from requests import Response

from cads_api_client.utils import get_link_href

# TODO add enum for status


logger = logging.getLogger(__name__)

# def cond_cached(func):
#     """
#     Cache response for a remote job request only if 'status' field is equal to 'successful' or 'failed'.
#     """
#     @functools.wraps(func)
#     def wrap(self, *args, **kwargs):
#         name = func.__name__
#         if not hasattr(self, name):
#             values = func(self, *args, **kwargs)
#             setattr(self, name, values)
#         else:
#             response = getattr(self, name)
#             if response.json()["status"] not in ('successful', 'failed'):
#                 response = func(self, *args, **kwargs)
#                 setattr(self, name, response)
#             response.raise_for_status()
#         return getattr(self, name)
#
#     return wrap
#


class Job:

    def __init__(self, job_id, base_url, session, headers, request=None, sleep_max=120):

        self.session = session
        self.headers = headers
        self.request = request
        self.sleep_max = sleep_max
        self.job_id = job_id
        self.base_url = base_url
        self.url = f"{self.base_url}/retrieve/v1/jobs/{job_id}"  # TODO from settings

    def __repr__(self):
        return f"Job(job_id={self.job_id})"

    @functools.cached_property
    def _response(self):
        return self.session.get(self.url)

    def json(self):
        return self._response.json()

    @functools.cached_property
    def id(self):
        return self.json().get("jobID")

    @functools.cached_property
    def _monitor_href(self):
        url = get_link_href(self._response, rel="monitor")
        return url

    @multiurl.robust
    def _monitor_response(self) -> Response:
        """
        Send request to get information about the remote job.
        """
        response = self.session.get(self._monitor_href, headers=self.headers)
        response.raise_for_status()
        return response

    @property
    def status(self) -> str:
        # note that this is the status of the job not of the job execution request
        return self._monitor_response().json().get("status")

    @property
    @multiurl.robust
    def results(self):
        if self.status not in ("successful", "failed"):
            raise Exception(f"Result not ready, job is {self.status}")
        try:
            results_url = get_link_href(response=self._monitor_response(), rel="results")
        except RuntimeError:
            results_url = f"{self._monitor_href}/results"
        results = self.session.get(results_url, headers=self.headers)  # , raise_for_status=False)
        return results

    @property
    def _result_href(self) -> str:
        if self.results.status_code != 200:
            raise KeyError("result_href not available for processing failed results")
        href = self.results.json()["asset"]["value"]["href"]
        assert isinstance(href, str)
        return href

    @property
    def _result_size(self) -> Optional[int]:
        asset = self.results.json().get("asset", {}).get("value", {})
        size = asset["file:size"]
        return int(size)

    @multiurl.robust
    def wait_on_results(self, retry_options: Dict[str, Any] = {}) -> None:
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
        last_status = self.status   # _robust_status(retry_options=retry_options)
        while True:
            status = self.status  # _robust_status(retry_options=retry_options)
            if last_status != status:
                logger.debug(f"status has been updated to {status}")
            if status == "successful":
                break
            elif status == "failed":
                # results = multiurl.robust(self._make_results, **retry_options)(self.url)
                info = self.results.json()
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
        return self.results

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
        url = urllib.parse.urljoin(self._monitor_href, self._result_href)

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
        size = self._result_size
        if size:
            if target_size != size:
                raise DownloadError(
                    "Download failed: downloaded %s byte(s) out of %s"
                    % (target_size, size)
                )
        return target_path


class ProcessingFailedError(RuntimeError):
    pass


class DownloadError(RuntimeError):
    pass
