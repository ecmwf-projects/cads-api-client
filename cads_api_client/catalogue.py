
import functools

from cads_api_client.processes import Process
from cads_api_client.utils import ConnectionObject


class Collection(ConnectionObject):
    def __init__(self, collection_id, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.collection_id = collection_id
        self.url = f"{self.base_url}/catalogue/v1/collections/{collection_id}"

    def __repr__(self):
        return f"Collection(collection_id={self.id})"

    @functools.cached_property
    def _response(self):
        return self.session.get(self.url)

    def json(self):
        return self._response.json()

    @property
    def id(self):
        return self._response.json().get("id")

    def retrieve_process(self) -> Process:
        return Process(self.id, base_url=self.base_url, session=self.session, headers=self.headers)
