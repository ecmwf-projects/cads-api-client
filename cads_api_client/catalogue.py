from typing import List

import attrs
import numpy as np
from owslib import ogcapi

from . import processing


@attrs.define
class Collection(processing.ApiResponse):
    def end_datetime(self) -> np.datetime64:
        try:
            end = self.json["extent"]["temporal"]["interval"][1]
        except Exception:
            end = "2022-07-20T23:00:00"
        return np.datetime64(end)

    def retrieve_process(self) -> processing.Process:
        hrefs = self.get_links_hrefs(rel="retrieve")
        if len(hrefs) != 1:
            raise RuntimeError("retrieve URL not found or not unique")
        return processing.Process.from_request("get", hrefs[0])

    def retrieve(self, **request) -> processing.Remote:
        return self.retrieve_process().execute(**request)


class Catalogue(ogcapi.Collections):  # type: ignore
    supported_api_version = "v1"

    def __init__(self, url, *args, **kwargs):
        url = f"{url}/{self.supported_api_version}"
        return super().__init__(url, *args, **kwargs)

    def collection_ids(self) -> List[str]:
        collections = self.collections()
        ids = [co["id"] for co in collections["collections"]]
        return ids

    def collection(self, collection_id: str) -> Collection:
        url = self._build_url(f"collections/{collection_id}")
        collection = Collection.from_request("get", url)
        return collection
