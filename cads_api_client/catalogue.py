
import functools

from cads_api_client.processes import Process


class Collection:
    def __init__(self, collection_id, base_url, session, headers):
        self.collection_id = collection_id
        self.base_url = base_url
        self.session = session
        self.headers = headers
        self.url = f"{base_url}/catalogue/v1/collections/{collection_id}"

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
