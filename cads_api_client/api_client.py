from __future__ import annotations

import functools
import os
from typing import Any, Dict, List, Optional

import attrs
import requests

import cads_api_client.jobs
from cads_api_client.api_response import ApiResponse
from cads_api_client.catalogue import Collections, Collection
from cads_api_client.jobs import Job, JobList
from cads_api_client.processes import ProcessList, Process


CADS_API_URL = os.getenv("CADS_API_URL", "http://localhost:8080/api")
CADS_API_KEY = os.getenv("CADS_API_KEY")

# TODO endpoints


@attrs.define(slots=False)
class ApiClient:
    """
    API client that interacts with Copernicus Data Store APIs. It allows to download data and get information about
    datasets, user profiles and available or running processes.
    """
    key: Optional[str] = CADS_API_KEY
    url: str = CADS_API_URL
    session: requests.Session = attrs.field(factory=requests.Session)

    def _headers(self) -> Dict[str, str]:
        if self.key is None:
            raise ValueError("A valid API key is needed to access this resource")
        return {"PRIVATE-TOKEN": self.key}

    @functools.cached_property
    def catalogue_api(self) -> CatalogueAPIClient:
        """
        Catalogue API client at `BASE_URL/catalogue`
        """
        return CatalogueAPIClient(
            f"{self.url}/catalogue", headers=self._headers(), session=self.session
        )

    @functools.cached_property
    def retrieve_api(self) -> ProcessesAPIClient:
        """
        Retrieve API client at `BASE_URL/retrieve`
        """
        return ProcessesAPIClient(
            f"{self.url}/retrieve", headers=self._headers(), session=self.session
        )

    @functools.cached_property
    def profile_api(self) -> ProfilesAPIClient:
        """
        Profile API client at `BASE_URL/profiles`
        """
        return ProfilesAPIClient(f"{self.url}/profiles", headers=self._headers())

    # CATALOGUE
    def collections(self, **params: Dict[str, Any]) -> catalogue.Collections:
        return self.catalogue_api.collections(params=params)

    def collection(self, collection_id: str) -> catalogue.Collection:
        return self.catalogue_api.collection(collection_id)

    # PROCESSES
    def processes(self, **params: Dict[str, Any]) -> processing.ProcessList:
        return self.retrieve_api.processes(params=params)

    def process(self, process_id: str) -> processing.Process:
        return self.retrieve_api.process(process_id=process_id)

    # JOBS
    def retrieve(
        self,
        collection_id: str,
        target: Optional[str] = None,
        retry_options: Dict[str, Any] = {},
        accepted_licences: List[Dict[str, Any]] = [],
        **request: Any,
    ) -> str:

        collection = self.collection(collection_id)
        return collection.retrieve(
            target,
            retry_options=retry_options,
            accepted_licences=accepted_licences,
            **request,
        )

    def get_requests(self, **params: Dict[str, Any]) -> cads_api_client.jobs.JobList:
        return self.retrieve_api.jobs(params=params)

    def get_request(self, request_uid: str) -> cads_api_client.jobs.StatusInfo:
        return self.retrieve_api.job(request_uid)

    def download_result(
        self, request_uid: str, target: Optional[str], retry_options: Dict[str, Any]
    ) -> str:
        return self.retrieve_api.download_result(
            request_uid, target, retry_options=retry_options
        )

    def valid_values(
        self, collection_id: str, request: Dict[str, Any]
    ) -> Dict[str, Any]:
        process = self.retrieve_api.process(collection_id)
        return process.valid_values(request)

    # LICENCES
    @property
    def licences(self) -> Dict[str, Any]:
        return self.catalogue_api.licenses()

    @property
    def accepted_licences(self) -> Dict[str, Any]:
        return self.profile_api.accepted_licences()

    def accept_licence(self, licence_id: str) -> Dict[str, Any]:
        return self.profile_api.accept_licence(licence_id)


class CatalogueAPIClient:
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

    def collections(self, params: Dict[str, Any] = {}) -> Collections:
        url = f"{self.url}/datasets"
        return Collections.from_request("get", url, params=params, session=self.session)

    def collection(self, collection_id: str) -> Collection:
        url = f"{self.url}/collections/{collection_id}"
        return Collection.from_request(
            "get", url, headers=self.headers, session=self.session
        )

    def licenses(self) -> Dict[str, Any]:
        url = f"{self.url}/vocabularies/licences"
        return ApiResponse.from_request(
            "get", url, headers=self.headers, session=self.session
        ).json


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

    # def process_execute(
    #     self, process_id: str, inputs: Dict[str, Any], **kwargs: Any
    # ) -> Job:
    #     assert "json" not in kwargs
    #     url = f"{self.url}/processes/{process_id}/execute"
    #     headers = kwargs.pop("headers", {})
    #     return Job.from_request(
    #         "post",
    #         url,
    #         json={"inputs": inputs},
    #         headers={**self.headers, **headers},
    #         session=self.session,
    #         **kwargs,
    #     )

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

    def job(self, job_id: str) -> Job:
        url = f"{self.url}/jobs/{job_id}"
        return Job.from_request(
            "get", url, headers=self.headers, session=self.session
        )


class ProfilesAPIClient:
    supported_api_version = "v1"

    def __init__(self, url: str, headers: Dict[str, Any] = {}) -> None:
        self.url = f"{url}/{self.supported_api_version}"
        self.headers = headers

    def profile(self) -> Dict[str, Any]:
        url = f"{self.url}/account"
        response = ApiResponse.from_request("get", url, headers=self.headers)
        return response.json

    def accept_licence(self, licence_id: str) -> Dict[str, Any]:
        url = f"{self.url}/account/licences/{licence_id}"
        response = ApiResponse.from_request("put", url, headers=self.headers)
        return response.json

    def accepted_licences(self) -> Dict[str, Any]:
        url = f"{self.url}/account/licences"
        response = ApiResponse.from_request("get", url, headers=self.headers)
        return response.json
