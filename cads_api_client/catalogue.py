from typing import Any, List

import attrs
import numpy as np
from owslib import ogcapi

from . import processing


@attrs.define
class Collections(processing.ApiResponse):
    def collection_ids(self) -> List[str]:
        return [collection["id"] for collection in self.json["collections"]]


@attrs.define
class Collection(processing.ApiResponse):
    def end_datetime(self) -> np.datetime64:
        try:
            end = self.json["extent"]["temporal"]["interval"][1]
        except Exception:
            end = "2022-07-20T23:00:00"
        return np.datetime64(end)

    def retrieve_process(self) -> processing.Process:
        url = self.get_link_href(rel="retrieve")
        return processing.Process.from_request("get", url)

    def retrieve(self, **request: Any) -> processing.Remote:
        return self.retrieve_process().execute(**request).make_remote()


class Catalogue(ogcapi.API):  # type: ignore
    supported_api_version = "v1"

    def __init__(self, url: str, *args: Any, **kwargs: Any) -> None:
        url = f"{url}/{self.supported_api_version}"
        super().__init__(url, *args, **kwargs)

    def collections(self) -> Collections:
        url = self._build_url("collections")
        return Collections.from_request("get", url)

    def collection(self, collection_id: str) -> Collection:
        url = self._build_url(f"collections/{collection_id}")
        return Collection.from_request("get", url)
