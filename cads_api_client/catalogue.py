from typing import List

from owslib import ogcapi


class Catalogue(ogcapi.Collections):  # type: ignore
    def collection_ids(self) -> List[str]:
        collections = self.collections()
        ids = [co["id"] for co in collections["collections"]]
        return ids
