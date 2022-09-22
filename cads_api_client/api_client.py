import functools
from typing import Any, Optional

import attrs

from . import catalogue, processing


@attrs.define(slots=False)
class ApiClient:
    url: str

    @functools.cached_property
    def catalogue_api(self):
        return catalogue.Catalogue(f"{self.url}/catalogue")

    @functools.cached_property
    def retrieve_api(self):
        return processing.Processing(f"{self.url}/retrieve")

    def collections(self) -> catalogue.Collections:
        return self.catalogue_api.collection()

    def collection(self, collection_id: str) -> catalogue.Collection:
        return self.catalogue_api.collection(collection_id)

    def retrieve(
        self, collection_id: str, target: Optional[str] = None, **request: Any
    ) -> str:
        collection = self.collection(collection_id)
        return collection.retrieve(target, **request)

    def get_requests(self) -> processing.JobList:
        return self.retrieve_api.jobs()

    def download_result(self, request_uid: str, target: Optional[str]) -> str:
        return self.retrieve_api.download_result(request_uid, target)
