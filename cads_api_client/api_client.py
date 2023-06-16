from __future__ import annotations

import os
from typing import Any, Dict, Optional

import attrs
import requests

from cads_api_client.api_response import ApiResponse
from cads_api_client.catalogue import Collections, Collection
from cads_api_client.jobs import Job, JobList
from cads_api_client.processes import ProcessList, Process

CADS_API_URL = os.getenv("CADS_API_URL", "http://localhost:8080/api")
CADS_API_KEY = os.getenv("CADS_API_KEY")


@attrs.define(slots=False)
class ApiClient:
    """
    API client that interacts with Copernicus Data Store APIs. It allows to download data and get information about
    datasets, user profiles and available or running processes.
    """
    session: requests.Session = attrs.field(factory=requests.Session)
    key: Optional[str] = CADS_API_KEY
    url: str = CADS_API_URL
    catalogue_dir = "catalogue"
    retrieve_dir = "retrieve"
    profiles_dir = "profiles"

    def _headers(self) -> Dict[str, str]:
        if self.key is None:
            raise ValueError("A valid API key is needed to access this resource")
        return {"PRIVATE-TOKEN": self.key}

    def collections(self, **params: Dict[str, Any]) -> Collections:
        url = f"{self.url}/{self.catalogue_dir}/datasets"
        return Collections.from_request("get", url, params=params, session=self.session)

    def collection(self, collection_id: str) -> Collection:
        url = f"{self.url}/{self.catalogue_dir}/collections/{collection_id}"
        return Collection.from_request(
            "get", url, headers=self._headers(), session=self.session
        )

    def processes(self, params: Dict[str, Any] = {}) -> ProcessList:
        url = f"{self.url}/{self.retrieve_dir}/processes"
        return ProcessList.from_request("get", url, params=params, session=self.session)

    def process(self, process_id: str) -> Process:
        url = f"{self.url}/{self.retrieve_dir}/processes/{process_id}"
        return Process.from_request(
            "get", url, headers=self._headers(), session=self.session
        )

    def jobs(self, params: Dict[str, Any] = {}) -> JobList:
        url = f"{self.url}/{self.retrieve_dir}//jobs"
        return JobList.from_request(
            "get", url, params=params, headers=self._headers(), session=self.session
        )

    def job(self, job_id: str) -> Job:
        url = f"{self.url}/{self.retrieve_dir}/jobs/{job_id}"
        return Job.from_request(
            "get", url, headers=self._headers(), session=self.session
        )

    def profile(self) -> Dict[str, Any]:
        url = f"{self.url}/{self.profiles_dir}/account"
        response = ApiResponse.from_request("get", url, headers=self._headers())
        return response.json

    @property
    def licences(self) -> Dict[str, Any]:
        url = f"{self.url}/{self.profiles_dir}/vocabularies/licences"
        return ApiResponse.from_request(
            "get", url, headers=self._headers(), session=self.session
        ).json

    @property
    def accepted_licences(self) -> Dict[str, Any]:
        url = f"{self.url}/{self.profiles_dir}/account/licences"
        response = ApiResponse.from_request("get", url, headers=self._headers())
        return response.json

    def accept_licence(self, licence_id: str) -> Dict[str, Any]:
        url = f"{self.url}/{self.profiles_dir}/account/licences/{licence_id}"
        response = ApiResponse.from_request("put", url, headers=self._headers())
        return response.json
