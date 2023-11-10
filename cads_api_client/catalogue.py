import datetime
from typing import Any, Dict, List, Optional

import attrs
import requests

from . import processing


@attrs.define
class Collections(processing.ApiResponse):
    def collection_ids(self) -> List[str]:
        return [collection["id"] for collection in self.json["collections"]]

    def next(self) -> Optional[processing.ApiResponse]:
        return self.from_rel_href(rel="next")

    def prev(self) -> Optional[processing.ApiResponse]:
        return self.from_rel_href(rel="prev")


@attrs.define
class Collection(processing.ApiResponse):
    headers: Dict[str, Any] = {}

    def end_datetime(self) -> datetime.datetime:
        try:
            end = self.json["extent"]["temporal"]["interval"][1]
        except Exception:
            end = "2022-07-20T23:00:00"
        return datetime.datetime.fromisoformat(end)

    @property
    def id(self) -> str:
        collection_id = self.json["id"]
        assert isinstance(collection_id, str)
        return collection_id

    def retrieve_process(self) -> processing.Process:
        url = self.get_link_href(rel="retrieve")
        return processing.Process.from_request(
            "get", url, headers=self.headers, session=self.session
        )

    def submit(self, **request: Any) -> processing.Remote:
        retrieve_process = self.retrieve_process()
        status_info = retrieve_process.execute(inputs=request, session=self.session)
        return status_info.make_remote()

    def retrieve(
        self,
        target: Optional[str] = None,
        retry_options: Dict[str, Any] = {},
        **request: Any,
    ) -> str:
        remote = self.submit(**request)
        return remote.download(target, retry_options=retry_options)


class Catalogue:
    supported_api_version = "v1"

    def __init__(
        self,
        url: str,
        force_exact_url: bool = False,
        headers: Dict[str, Any] = {},
        session: requests.Session = requests.api,  # type: ignore
    ) -> None:
        if not force_exact_url:
            url = f"{url}/{self.supported_api_version}"
        self.url = url
        self.headers = headers
        self.session = session

    def collections(self, params: Dict[str, Any] = {}) -> Collections:
        url = f"{self.url}/datasets"
        return Collections.from_request("get", url, params=params, session=self.session)

    def collection(self, collection_id: str) -> Collection:
        url = f"{self.url}/collections/{collection_id}"
        return Collection.from_request(
            "get", url, headers=self.headers, session=self.session
        )

    def licenses(self) -> Dict[str, Any]:
        url = f"{self.url}/vocabularies/licences"
        return processing.ApiResponse.from_request(
            "get", url, headers=self.headers, session=self.session
        ).json
