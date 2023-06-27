from __future__ import annotations

from typing import Any, Dict, Optional, List

import attrs
import requests

from cads_api_client.catalogue import Collection
from cads_api_client.jobs import Job
from cads_api_client.multi_retrieve import multi_retrieve
from cads_api_client.processes import Process
from cads_api_client.settings import CATALOGUE_DIR, PROFILES_DIR, RETRIEVE_DIR, API_VERSION, CADS_API_KEY, CADS_API_URL
from cads_api_client.utils import ResponseIterator, ConnectionObject


@attrs.define(slots=False)
class APIClient(ConnectionObject):
    """
    API client that interacts with Copernicus Data Store APIs. It allows to download data and get information about
    datasets (collections), available processes that can be instantiated, remote jobs with their status and user 
    profiles.
    """
    session: requests.Session = requests.Session()
    base_url: str = CADS_API_URL
    api_key: str = CADS_API_KEY

    def collections(self, params: Dict[str, Any] = None):     # TODO limit parameter
        url = f"{self.base_url}/{CATALOGUE_DIR}/v{API_VERSION}/datasets"
        for page in ResponseIterator(url, session=self.session, headers=self.headers, params=params):
            for collection in page.json()["collections"]:
                yield Collection(collection_id=collection["id"],
                                 base_url=self.base_url, session=self.session, api_key=self.api_key)

    def collection(self, collection_id: str) -> Collection:
        return Collection(collection_id,
                          base_url=self.base_url, session=self.session, api_key=self.api_key)

    def processes(self, params: Dict[str, Any] = {}):
        url = f"{self.base_url}/{RETRIEVE_DIR}/v{API_VERSION}/processes"
        for page in ResponseIterator(url, session=self.session, headers=self.headers, params=params):
            for process in page.json()["processes"]:
                yield Process(pid=process["id"],
                              session=self.session, api_key=self.api_key)

    def process(self, process_id: str) -> Process:
        return Process(pid=process_id, base_url=self.base_url, headers=self.headers, session=self.session)

    def jobs(self, params: Dict[str, Any] = {}):
        url = f"{self.base_url}/{RETRIEVE_DIR}/v{API_VERSION}/jobs"
        for page in ResponseIterator(url, session=self.session, headers=self.headers, params=params):
            for job in page.json()["jobs"]:
                print(job["jobID"])
                yield Job(job_id=job["jobID"],
                          base_url=self.base_url, session=self.session, api_key=self.api_key)

    def job(self, job_id: str) -> Job:
        return Job(job_id, base_url=self.base_url, headers=self.headers, session=self.session)

    def retrieve(self, collection_id: str, inputs: List[dict], accepted_licences: List[dict],
                 target: Optional[str] = None,
                 max_updates: Optional[int] = 10, max_downloads: Optional[int] = 2):
        """
        Execute multiple downloads concurrently.

        Submit jobs to the backend, updates their status and download the requested data concurrently in a
        multithreading pool. The number of concurrent updates and downloads can be controlled by setting the
        appropriate parameters.

        Parameters
        ----------
        collection_id: Collection id
        inputs: List of requests to be submitted to the retrieve api. The accepted values can be found by calling the
         :meth:`Process.valid_values` method for :class:`Process`.
        accepted_licences
        target: Local download folder
        max_updates: Number of max concurrent job status updates
        max_downloads: Number of max concurrent downloads

        Returns
        -------
        List of results

        """
        collection = self.collection(collection_id=collection_id)
        if len(inputs) == 1:
            job = collection.retrieve_process()\
                             .execute(inputs=inputs[0], accepted_licences=accepted_licences)
            job.wait_on_results()
            return job.download(target=target)     # TODO auto wait
        else:
            results = multi_retrieve(collection=collection, requests=inputs, accepted_licenses=accepted_licences,
                                     target=target, max_updates=max_updates, max_downloads=max_downloads)
        return results

    def profile(self) -> Dict[str, Any]:
        url = f"{self.base_url}/{PROFILES_DIR}/v{API_VERSION}/account"
        response = self.session.request("get", url, headers=self.headers)
        return response.json()

    @property
    def licences(self) -> Dict[str, Any]:
        url = f"{self.base_url}/{PROFILES_DIR}/v{API_VERSION}/vocabularies/licences"
        response = self.session.request("get", url, headers=self.headers)
        return response.json()

    @property
    def accepted_licences(self) -> Dict[str, Any]:
        url = f"{self.base_url}/{PROFILES_DIR}/v{API_VERSION}/account/licences"
        response = self.session.request("get", url, headers=self.headers)
        return response.json()

    def accept_licence(self, licence_id: str) -> Dict[str, Any]:
        url = f"{self.base_url}/{PROFILES_DIR}/v{API_VERSION}/account/licences/{licence_id}"
        response = self.session.request("put", url, headers=self.headers)
        return response.json()
