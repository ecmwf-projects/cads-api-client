import functools
from typing import Any, Dict, Optional

import attrs
import requests

from . import catalogue, config, processing, profile


@attrs.define(slots=False)
class ApiClient:
    key: Optional[str] = None
    url: Optional[str] = None
    session: requests.Session = attrs.field(factory=requests.Session)
    sleep_max: int = 120

    def get_url(self) -> str:
        return self.url or config.get_config("url")

    def get_key(self) -> str:
        return self.key or config.get_config("key")

    def _headers(self) -> Dict[str, str]:
        key = self.get_key()
        if key is None:
            raise ValueError("A valid API key is needed to access this resource")
        return {"PRIVATE-TOKEN": key}

    @functools.cached_property
    def catalogue_api(self) -> catalogue.Catalogue:
        return catalogue.Catalogue(
            f"{self.get_url()}/catalogue", headers=self._headers(), session=self.session
        )

    @functools.cached_property
    def retrieve_api(self) -> processing.Processing:
        return processing.Processing(
            f"{self.get_url()}/retrieve",
            headers=self._headers(),
            session=self.session,
            sleep_max=self.sleep_max,
        )

    @functools.cached_property
    def profile_api(self) -> profile.Profile:
        return profile.Profile(f"{self.get_url()}/profiles", headers=self._headers())

    def collections(self, **params: Dict[str, Any]) -> catalogue.Collections:
        return self.catalogue_api.collections(params=params)

    def collection(self, collection_id: str) -> catalogue.Collection:
        return self.catalogue_api.collection(collection_id)

    def processes(self, **params: Dict[str, Any]) -> processing.ProcessList:
        return self.retrieve_api.processes(params=params)

    def process(self, process_id: str) -> processing.Process:
        return self.retrieve_api.process(process_id=process_id)

    def retrieve(
        self,
        collection_id: str,
        target: Optional[str] = None,
        retry_options: Dict[str, Any] = {},
        **request: Any,
    ) -> str:
        collection = self.collection(collection_id)
        return collection.retrieve(
            target,
            retry_options=retry_options,
            **request,
        )

    def submit_and_wait_on_result(
        self, collection_id: str, retry_options: Dict[str, Any] = {}, **request: Any
    ) -> processing.Results:
        return self.retrieve_api.submit_and_wait_on_result(
            collection_id, retry_options=retry_options, **request
        )

    def get_requests(self, **params: Dict[str, Any]) -> processing.JobList:
        return self.retrieve_api.jobs(params=params)

    def get_request(self, request_uid: str) -> processing.StatusInfo:
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

    @property
    def licences(self) -> Dict[str, Any]:
        return self.catalogue_api.licenses()

    @property
    def accepted_licences(self) -> Dict[str, Any]:
        return self.profile_api.accepted_licences()

    def accept_licence(self, licence_id: str, revision: int) -> Dict[str, Any]:
        return self.profile_api.accept_licence(licence_id, revision=revision)
