import functools
from typing import Any, Dict, Optional

import attrs

from . import catalogue, processing


@attrs.define(slots=False)
class ApiClient:
    url: str

    @functools.cached_property
    def catalogue_api(self) -> catalogue.Catalogue:
        return catalogue.Catalogue(f"{self.url}/catalogue")

    @functools.cached_property
    def retrieve_api(self) -> processing.Processing:
        return processing.Processing(f"{self.url}/retrieve")

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
