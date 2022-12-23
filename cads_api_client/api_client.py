import functools
import os
from typing import Any, Dict, Optional

import attrs

from . import catalogue, processing, profile

CADS_API_URL = os.getenv("CADS_API_URL", "http://localhost:8080/api")
CADS_API_KEY = os.getenv("CADS_API_KEY")


@attrs.define(slots=False)
class ApiClient:
    key: Optional[str] = CADS_API_KEY
    url: str = CADS_API_URL

    def _headers(self) -> Dict[str, str]:
        if self.key is None:
            raise ValueError("A valid API key is needed to access this resource")
        return {"PRIVATE-TOKEN": self.key}

    @functools.cached_property
    def catalogue_api(self) -> catalogue.Catalogue:
        return catalogue.Catalogue(f"{self.url}/catalogue", headers=self._headers())

    @functools.cached_property
    def retrieve_api(self) -> processing.Processing:
        return processing.Processing(f"{self.url}/retrieve", headers=self._headers())

    @functools.cached_property
    def profile_api(self) -> profile.Profile:
        return profile.Profile(f"{self.url}/profiles", headers=self._headers())

    def collections(self) -> catalogue.Collections:
        return self.catalogue_api.collections()

    def collection(self, collection_id: str) -> catalogue.Collection:
        return self.catalogue_api.collection(collection_id)

    def retrieve(
        self,
        collection_id: str,
        target: Optional[str] = None,
        retry_options: Dict[str, Any] = {},
        **request: Any,
    ) -> str:
        collection = self.collection(collection_id)
        return collection.retrieve(target, retry_options=retry_options, **request)

    def get_requests(self) -> processing.JobList:
        return self.retrieve_api.jobs()

    def get_request(self, request_uid: str) -> processing.StatusInfo:
        return self.retrieve_api.job(request_uid)

    def download_result(
        self, request_uid: str, target: Optional[str], retry_options: Dict[str, Any]
    ) -> str:
        return self.retrieve_api.download_result(
            request_uid, target, retry_options=retry_options
        )

    def licences(self):
        return self.catalogue_api.licenses()

    @property
    def accepted_licences(self) -> dict[str, Any]:
        return self.profile_api.accepted_licences()

    def accept_licence(self, licence_id: str) -> dict[str, Any]:
        return self.profile_api.accept_licence(licence_id)

