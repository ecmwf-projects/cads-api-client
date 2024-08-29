from __future__ import annotations

import datetime
from typing import Any

try:
    from typing import Self
except ImportError:
    from typing_extensions import Self

import attrs
import requests

from . import processing


@attrs.define
class Collections(processing.ApiResponse):
    def collection_ids(self) -> list[str]:
        return [collection["id"] for collection in self.json["collections"]]

    def next(self) -> Self | None:
        return self.from_rel_href(rel="next")

    def prev(self) -> Self | None:
        return self.from_rel_href(rel="prev")


@attrs.define
class Collection(processing.ApiResponse):
    @property
    def temporal_interval(self) -> tuple[str, str]:
        ((begin, end),) = self.json["extent"]["temporal"]["interval"]
        return (str(begin), str(end))

    @property
    def begin_datetime(self) -> datetime.datetime:
        return datetime.datetime.fromisoformat(self.temporal_interval[0])

    @property
    def end_datetime(self) -> datetime.datetime:
        return datetime.datetime.fromisoformat(self.temporal_interval[1])

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
        target: str | None = None,
        retry_options: dict[str, Any] = {},
        **request: Any,
    ) -> str:
        remote = self.submit(**request)
        return remote.download(target, retry_options=retry_options)


@attrs.define(slots=False)
class Catalogue:
    url: str
    force_exact_url: bool = False
    headers: dict[str, Any] = {}
    session: requests.Session = attrs.field(factory=requests.Session)
    supported_api_version: str = "v1"

    def __attrs_post_init__(self) -> None:
        if not self.force_exact_url:
            self.url += f"/{self.supported_api_version}"

    def collections(self, params: dict[str, Any] = {}) -> Collections:
        url = f"{self.url}/datasets"
        return Collections.from_request("get", url, params=params, session=self.session)

    def collection(self, collection_id: str) -> Collection:
        url = f"{self.url}/collections/{collection_id}"
        return Collection.from_request(
            "get", url, headers=self.headers, session=self.session
        )

    def licenses(self) -> dict[str, Any]:
        url = f"{self.url}/vocabularies/licences"
        return processing.ApiResponse.from_request(
            "get", url, headers=self.headers, session=self.session
        ).json
