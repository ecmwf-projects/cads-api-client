import datetime
from typing import Any, Dict, List, Optional

import attrs

from . import processing


@attrs.define
class Collections(processing.ApiResponse):
    def collection_ids(self) -> List[str]:
        return [collection["id"] for collection in self.json["collections"]]

    def next(self) -> processing.T_ApiResponse:
        next_page = self.get_links(rel="next")
        assert len(next_page) <= 1
        if len(next_page) == 1:
            out = self.from_request("get", url=next_page[0]["href"])
        else:
            out = None
        return out

    def prev(self) -> processing.T_ApiResponse:
        prev_page = self.get_links(rel="prev")
        assert len(prev_page) <= 1
        if len(prev_page) == 1:
            out = self.from_request("get", url=prev_page[0]["href"])
        else:
            out = None
        return out


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
        return processing.Process.from_request("get", url, headers=self.headers)

    def submit(self, **request: Any) -> processing.Remote:
        retrieve_process = self.retrieve_process()
        status_info = retrieve_process.execute(inputs=request)
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
        self, url: str, force_exact_url: bool = False, headers: Dict[str, Any] = {}
    ) -> None:
        if not force_exact_url:
            url = f"{url}/{self.supported_api_version}"
        self.url = url
        self.headers = headers

    def collections(self, limit=None, **params) -> Collections:
        url = f"{self.url}/datasets"
        if limit is not None:
            params = {"limit": limit, **params}
        return Collections.from_request("get", url, params=params)

    def collection(self, collection_id: str) -> Collection:
        url = f"{self.url}/collections/{collection_id}"
        return Collection.from_request("get", url, headers=self.headers)
