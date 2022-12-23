import functools
import os
from typing import Any, Optional

import attrs

from . import catalogue, processing, profile

CADS_API_URL = os.getenv("CADS_API_URL", "http://localhost:8080/api")
CADS_API_KEY = os.getenv("CADS_API_KEY")


@attrs.define(slots=False)
class ApiClient:
    key: Optional[str] = CADS_API_KEY
    url: str = CADS_API_URL

    def _headers(self) -> dict[str, str]:
        if self.key is None:
            raise ValueError("A valid API key is needed to access this resource")
        return {"PRIVATE-TOKEN": self.key}

    @functools.cached_property
    def catalogue_api(self) -> catalogue.Catalogue:
        return catalogue.Catalogue(f"{self.url}/catalogue", headers=self._headers())

    @functools.cached_property
    def retrieve_api(self) -> processing.Processing:
        return processing.Processing(f"{self.url}/retrieve", headers=self._headers())

    def collections(self, **params: dict[str, Any]) -> catalogue.Collections:
        return self.catalogue_api.collections(params=params)

    def collection(self, collection_id: str) -> catalogue.Collection:
        return self.catalogue_api.collection(collection_id)

    @functools.cached_property
    def profile_api(self) -> profile.Profile:
        return profile.Profile(f"{self.url}/profiles", headers=self._headers())

    def processes(self, **params: dict[str, Any]) -> processing.ProcessList:
        return self.retrieve_api.processes(params=params)

    def process(self, process_id: str) -> processing.Process:
        return self.retrieve_api.process(process_id=process_id)

    def retrieve(
        self,
        collection_id: str,
        target: Optional[str] = None,
        retry_options: dict[str, Any] = {},
        accepted_licences: list[dict[str, Any]] = [],
        **request: Any,
    ) -> str:
        collection = self.collection(collection_id)
        return collection.retrieve(
            target,
            retry_options=retry_options,
            accepted_licences=accepted_licences,
            **request,
        )

    def get_requests(self, **params: dict[str, Any]) -> processing.JobList:
        return self.retrieve_api.jobs(params=params)

    def get_request(self, request_uid: str) -> processing.StatusInfo:
        return self.retrieve_api.job(request_uid)

    def download_result(
        self, request_uid: str, target: Optional[str], retry_options: dict[str, Any]
    ) -> str:
        return self.retrieve_api.download_result(
            request_uid, target, retry_options=retry_options
        )

    def valid_values(
        self, collection_id: str, request: dict[str, Any]
    ) -> dict[str, Any]:
        process = self.retrieve_api.process(collection_id)
        return process.valid_values(request)

    @property
    def licences(self) -> dict[str, Any]:
        return self.catalogue_api.licenses()

    @property
    def accepted_licences(self) -> dict[str, Any]:
        return self.profile_api.accepted_licences()

    def accept_licence(self, licence_id: str) -> dict[str, Any]:
        return self.profile_api.accept_licence(licence_id)
