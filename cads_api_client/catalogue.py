
import functools

from cads_api_client.processes import Process
from cads_api_client.settings import CATALOGUE_DIR, API_VERSION
from cads_api_client.utils import ConnectionObject


class Collection(ConnectionObject):
    def __init__(self, collection_id: str, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.collection_id = collection_id
        self.url = f"{self.base_url}/{CATALOGUE_DIR}/v{API_VERSION}/collections/{collection_id}"

    def __repr__(self):
        return f"Collection(collection_id={self.id})"

    @functools.cached_property
    def _response(self):
        return self.session.get(self.url)

    def json(self):
        return self._response.json()

    @property
    def id(self):
        # return self._response.json().get("id")
        return self.collection_id

    def retrieve_process(self) -> Process:
        return Process(pid=self.id, base_url=self.base_url, session=self.session, api_key=self.api_key)
