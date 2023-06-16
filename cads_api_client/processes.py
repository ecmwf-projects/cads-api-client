from __future__ import annotations

import functools
import logging
from typing import Any, Dict, List, Optional, TypeVar

import attrs
import requests

from cads_api_client.api_response import ApiResponse
from cads_api_client.jobs import Job

T_ApiResponse = TypeVar("T_ApiResponse", bound="ApiResponse")

logger = logging.Logger(__name__)



class ProcessingFailedError(RuntimeError):
    pass


class DownloadError(RuntimeError):
    pass

# TODO as iterator
@attrs.define
class ProcessList(ApiResponse):
    def process_ids(self) -> List[str]:
        return [proc["id"] for proc in self.json["processes"]]

    def next(self) -> Optional[ApiResponse]:
        return self.from_rel_href(rel="next")

    def prev(self) -> Optional[ApiResponse]:
        return self.from_rel_href(rel="prev")

    def __repr__(self):
        # links = {l["rel"]: l.get("href") for l in self.json["links"]}
        processes = '\n'.join([f"- {p['id']} (v{p['version']})" for p in self.json["processes"]])
        return processes


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
        accepted_licences: List[Dict[str, Any]] = [],
        **kwargs: Any,
    ) -> StatusInfo:
        assert "json" not in kwargs
        url = f"{self.response.request.url}/execute"
        json = {"inputs": inputs, "acceptedLicences": accepted_licences}
        return StatusInfo.from_request(
            "post", url, json=json, headers=self.headers, **kwargs
        )

    def valid_values(self, request: Dict[str, Any] = {}) -> Dict[str, Any]:
        url = f"{self.response.request.url}/constraints"
        response = ApiResponse.from_request("post", url, json={"inputs": request})
        response.response.raise_for_status()
        return response.json




def cond_cached(func):
    """
    Cache response for a remote job request only if 'status' field is equal to 'successful' or 'failed'.
    """
    @functools.wraps(func)
    def wrap(self, *args, **kwargs):
        name = "_" + func.__name__
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




class ProcessesAPIClient:
    """
    Processing API client. Interact with processes and jobs
    Call the /processes and /jobs endpoint
    """
    supported_api_version = "v1"

    def __init__(
        self,
        url: str,
        force_exact_url: bool = False,
        headers: Dict[str, Any] = {},
        session: requests.Session = requests.api,  # type: ignore
    ) -> None:
        if not force_exact_url:
            url = f"{url}/{self.supported_api_version}"
        self.url = url
        self.headers = headers
        self.session = session

    def processes(self, params: Dict[str, Any] = {}) -> ProcessList:
        url = f"{self.url}/processes"
        return ProcessList.from_request("get", url, params=params, session=self.session)

    def process(self, process_id: str) -> Process:
        url = f"{self.url}/processes/{process_id}"
        return Process.from_request(
            "get", url, headers=self.headers, session=self.session
        )

    def process_execute(
        self, process_id: str, inputs: Dict[str, Any], **kwargs: Any
    ) -> Job:
        assert "json" not in kwargs
        url = f"{self.url}/processes/{process_id}/execute"
        headers = kwargs.pop("headers", {})
        return StatusInfo.from_request(
            "post",
            url,
            json={"inputs": inputs},
            headers={**self.headers, **headers},
            session=self.session,
            **kwargs,
        )

    # def jobs(self, params: Dict[str, Any] = {}) -> JobList:
    #     url = f"{self.url}/jobs"
    #     return JobList.from_request(
    #         "get", url, params=params, headers=self.headers, session=self.session
    #     )
    #
    # def job(self, job_id: str) -> StatusInfo:
    #     url = f"{self.url}/jobs/{job_id}"
    #     return StatusInfo.from_request(
    #         "get", url, headers=self.headers, session=self.session
    #     )
    #
    # def job_results(self, job_id: str) -> Results:
    #     url = f"{self.url}/jobs/{job_id}/results"
    #     return Results.from_request(
    #         "get", url, headers=self.headers, session=self.session
    #     )

    # convenience methods

    # def make_remote(self, job_id: str) -> JobsAPIClient:
    #     url = f"{self.url}/jobs/{job_id}"
    #     return JobsAPIClient(url, headers=self.headers, session=self.session)

    # def download_result(
    #     self, job_id: str, target: Optional[str], retry_options: Dict[str, Any]
    # ) -> str:
    #     # NOTE: the remote waits for the result to be available
    #     return self.make_remote(job_id).download(target, retry_options=retry_options)
