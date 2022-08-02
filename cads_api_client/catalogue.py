from typing import List

import numpy as np
from owslib import ogcapi

from . import processing


class Collection(processing.ApiResponse):
    def end_datetime(self) -> np.datetime64:
        try:
            end = self.json["extent"]["temporal"]["interval"][1]
        except Exception:
            end = "2022-07-20T23:00:00"
        return np.datetime64(end)

    def retrieve_process(self) -> processing.Process:
        for link in self.json.get("links", []):
            if link.get("rel") == "retrieve-process":
                href = link["href"]
                return processing.Process.from_request("get", href)
        raise RuntimeError('No link with rel="retrieve-process"')

    def retrieve(self, **request) -> processing.Remote:
        return self.retrieve_process().execute(**request)


class Catalogue(ogcapi.Collections):  # type: ignore
    def collection_ids(self) -> List[str]:
        collections = self.collections()
        ids = [co["id"] for co in collections["collections"]]
        return ids

    def collection(self, collection_id: str) -> Collection:
        url = self._build_url(f"collections/{collection_id}")
        collection = Collection.from_request("get", url)
        return collection
