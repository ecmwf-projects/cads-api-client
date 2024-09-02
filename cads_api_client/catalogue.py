from __future__ import annotations

import datetime
from typing import Any

try:
    from typing import Self
except ImportError:
    from typing_extensions import Self

import attrs
import requests

from . import config, processing


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
        begin, end = map(str, self.json["extent"]["temporal"]["interval"][0])
        return (begin, end)

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

    def retrieve_process(
        self, headers: dict[str, str] | None = None
    ) -> processing.Process:
        url = self.get_link_href(rel="retrieve")
        return processing.Process.from_request(
            "get",
            url,
            headers=self.headers if headers is None else headers,
            session=self.session,
            retry_options=self.retry_options,
        )

    def submit(
        self, headers: dict[str, str] | None = None, **request: Any
    ) -> processing.Remote:
        retrieve_process = self.retrieve_process(headers=headers)
        status_info = retrieve_process.execute(inputs=request)
        return status_info.make_remote()

    def retrieve(
        self,
        target: str | None = None,
        headers: dict[str, str] | None = None,
        **request: Any,
    ) -> str:
        remote = self.submit(headers=headers, **request)
        return remote.download(target)


@attrs.define(slots=False)
class Catalogue:
    url: str
    force_exact_url: bool = False
    headers: dict[str, Any] = {}
    session: requests.Session = attrs.field(factory=requests.Session)
    retry_options: dict[str, Any] = {}

    def __attrs_post_init__(self) -> None:
        if not self.force_exact_url:
            self.url += f"/{config.SUPPORTED_API_VERSION}"

    def collections(self, params: dict[str, Any] = {}) -> Collections:
        url = f"{self.url}/datasets"
        return Collections.from_request(
            "get",
            url,
            params=params,
            headers=self.headers,
            session=self.session,
            retry_options=self.retry_options,
        )

    def collection(self, collection_id: str) -> Collection:
        url = f"{self.url}/collections/{collection_id}"
        return Collection.from_request(
            "get",
            url,
            headers=self.headers,
            session=self.session,
            retry_options=self.retry_options,
        )

    def licenses(self) -> dict[str, Any]:
        url = f"{self.url}/vocabularies/licences"
        return processing.ApiResponse.from_request(
            "get",
            url,
            headers=self.headers,
            session=self.session,
            retry_options=self.retry_options,
        ).json
