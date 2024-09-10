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
        return datetime.datetime.fromisoformat(
            self.temporal_interval[0].replace("Z", "+00:00")
        )

    @property
    def end_datetime(self) -> datetime.datetime:
        return datetime.datetime.fromisoformat(
            self.temporal_interval[1].replace("Z", "+00:00")
        )

    @property
    def id(self) -> str:
        collection_id = self.json["id"]
        assert isinstance(collection_id, str)
        return collection_id

    def retrieve_process(
        self, headers: dict[str, str] | None = None
    ) -> processing.Process:
        url = self.get_link_href(rel="retrieve")
        kwargs = self.request_kwargs
        if headers is not None:
            kwargs["headers"] = headers
        return processing.Process.from_request("get", url, **kwargs)

    def submit(
        self,
        **request: Any,
    ) -> processing.Remote:
        retrieve_process = self.retrieve_process(headers=self.headers)
        status_info = retrieve_process.execute(inputs=request)
        return status_info.make_remote()

    def retrieve(
        self,
        target: str | None = None,
        **request: Any,
    ) -> str:
        remote = self.submit(**request)
        return remote.download(target)


@attrs.define(slots=False)
class Catalogue:
    url: str
    headers: dict[str, Any]
    session: requests.Session
    retry_options: dict[str, Any]
    request_options: dict[str, Any]
    download_options: dict[str, Any]
    sleep_max: int
    cleanup: bool
    force_exact_url: bool = False

    def __attrs_post_init__(self) -> None:
        if not self.force_exact_url:
            self.url += f"/{config.SUPPORTED_API_VERSION}"

    @property
    def request_kwargs(self) -> processing.RequestKwargs:
        return processing.RequestKwargs(
            headers=self.headers,
            session=self.session,
            retry_options=self.retry_options,
            request_options=self.request_options,
            download_options=self.download_options,
            sleep_max=self.sleep_max,
            cleanup=self.cleanup,
        )

    def collections(self, params: dict[str, Any] = {}) -> Collections:
        url = f"{self.url}/datasets"
        return Collections.from_request(
            "get", url, params=params, **self.request_kwargs
        )

    def collection(self, collection_id: str) -> Collection:
        url = f"{self.url}/collections/{collection_id}"
        return Collection.from_request("get", url, **self.request_kwargs)

    def licenses(self) -> dict[str, Any]:
        url = f"{self.url}/vocabularies/licences"
        return processing.ApiResponse.from_request(
            "get", url, **self.request_kwargs
        ).json
