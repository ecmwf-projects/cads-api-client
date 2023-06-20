from __future__ import annotations

from typing import Any, Dict, Optional, List

import attrs
import requests

from cads_api_client.catalogue import Collection
from cads_api_client.jobs import Job
from cads_api_client.multi_retrieve import multi_retrieve
from cads_api_client.processes import Process
from cads_api_client.settings import CADS_API_URL, CADS_API_KEY, CATALOGUE_DIR, PROFILES_DIR, RETRIEVE_DIR
from cads_api_client.utils import ResponseIterator


@attrs.define(slots=False)
class ApiClient:
    """
    API client that interacts with Copernicus Data Store APIs. It allows to download data and get information about
    datasets, user profiles and available or running processes.
    """
    session: requests.Session = attrs.field(factory=requests.Session)
    key: Optional[str] = CADS_API_KEY
    base_url: str = CADS_API_URL
    version: int = 1
    catalogue_dir = CATALOGUE_DIR
    retrieve_dir = RETRIEVE_DIR
    profiles_dir = PROFILES_DIR
    # catalogue_url = f"{base_url}/{catalogue_dir}/v{version}"

    def _headers(self) -> Dict[str, str]:
        if self.key is None:
            raise ValueError("A valid API key is needed to access this resource")
        return {"PRIVATE-TOKEN": self.key}

    def collections(self, **params: Dict[str, Any]):
        url = f"{self.base_url}/{self.catalogue_dir}/v{self.version}/datasets"
        for page in ResponseIterator(url, session=self.session, headers=self._headers()):
            for collection in page.json()["collections"]:
                yield Collection(collection_id=collection["id"],
                                 base_url=self.base_url, session=self.session, headers=self._headers())

    def collection(self, collection_id: str) -> Collection:
        return Collection(collection_id,
                          base_url=self.base_url, session=self.session, api_key=self.key)

    def processes(self, params: Dict[str, Any] = {}):
        url = f"{self.base_url}/{self.retrieve_dir}/v{self.version}/processes"
        for page in ResponseIterator(url, session=self.session, headers=self._headers()):
            for process in page.json()["processes"]:
                yield Process(pid=process["id"],
                              session=self.session, headers=self._headers())

    def process(self, process_id: str) -> Process:
        return Process(pid=process_id, base_url=self.base_url, headers=self._headers(), session=self.session)

    def jobs(self):
        url = f"{self.base_url}/{self.retrieve_dir}/v{self.version}/jobs"
        for page in ResponseIterator(url, session=self.session, headers=self._headers()):
            for job in page.json()["jobs"]:
                yield Job(job_id=job["jobID"],
                          base_url=self.base_url, session=self.session, headers=self._headers())

    def job(self, job_id: str) -> Job:
        return Job(job_id, base_url=self.base_url, headers=self._headers(), session=self.session)

    def retrieve(self, collection_id: str, requests: List[dict], accepted_licenses: List[dict],
                 target: Optional[str] = None,
                 max_updates: Optional[int] = 10, max_downloads: Optional[int] = 2):
        collection = self.collection(collection_id=collection_id)
        if len(requests) == 1:
            return collection.retrieve_process()\
                             .execute(inputs=requests[0], accepted_licences=accepted_licenses)\
                             .download(target=target)
        else:
            results = multi_retrieve(collection=collection, requests=requests, accepted_licenses=accepted_licenses,
                                     target=target, max_updates=max_updates, max_downloads=max_downloads)
        return results

    def profile(self) -> Dict[str, Any]:
        url = f"{self.base_url}/{self.profiles_dir}/v{self.version}/account"
        response = self.session.request("get", url, headers=self._headers())
        return response.json()

    @property
    def licences(self) -> Dict[str, Any]:
        url = f"{self.base_url}/{self.profiles_dir}/v{self.version}/vocabularies/licences"
        response = self.session.request("get", url, headers=self._headers())
        return response.json()

    @property
    def accepted_licences(self) -> Dict[str, Any]:
        url = f"{self.base_url}/{self.profiles_dir}/v{self.version}/account/licences"
        response = self.session.request("get", url, headers=self._headers())
        return response.json()

    def accept_licence(self, licence_id: str) -> Dict[str, Any]:
        url = f"{self.base_url}/{self.profiles_dir}/v{self.version}/account/licences/{licence_id}"
        response = self.session.request("put", url, headers=self._headers())
        return response.json()
