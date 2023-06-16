import functools
import os
from typing import Any, Dict, List, Optional

import attrs
import requests

import cads_api_client.jobs
from . import catalogue, processing, profile

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
    def catalogue_api(self) -> catalogue.CatalogueAPIClient:
        """
        Catalogue API client at `BASE_URL/catalogue`
        """
        return catalogue.CatalogueAPIClient(
            f"{self.url}/catalogue", headers=self._headers(), session=self.session
        )

    @functools.cached_property
    def retrieve_api(self) -> processing.ProcessesAPIClient:
        """
        Retrieve API client at `BASE_URL/retrieve`
        """
        return processing.ProcessesAPIClient(
            f"{self.url}/retrieve", headers=self._headers(), session=self.session
        )

    @functools.cached_property
    def profile_api(self) -> profile.Profile:
        """
        Profile API client at `BASE_URL/profiles`
        """
        return profile.Profile(f"{self.url}/profiles", headers=self._headers())

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
