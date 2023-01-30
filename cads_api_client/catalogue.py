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

        process = processing.Process.from_request(
            "get",
            url,
            headers=self.headers,
            session=self.session,
            maximum_tries=self.maximum_tries,
            retry_after=self.retry_after,
        )
        return process

    def submit(
        self, accepted_licences: List[Dict[str, Any]] = [], **request: Any
    ) -> processing.Remote:
        retrieve_process = self.retrieve_process()
        status_info = retrieve_process.execute(
            inputs=request, accepted_licences=accepted_licences, session=self.session
        )
        return status_info.make_remote()

    def retrieve(
        self,
        target: Optional[str] = None,
        accepted_licences: List[Dict[str, Any]] = [],
        **request: Any,
    ) -> str:
        remote = self.submit(accepted_licences=accepted_licences, **request)
        return remote.download(target)


class Catalogue:
    supported_api_version = "v1"
    maximum_tries: int = 500
    retry_after: int = 120

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
        return Collections.from_request(
            "get",
            url,
            params=params,
            session=self.session,
            maximum_tries=self.maximum_tries,
            retry_after=self.retry_after,
        )

    def collection(self, collection_id: str) -> Collection:
        url = f"{self.url}/collections/{collection_id}"
        return Collections.from_request(
            "get",
            url,
            headers=self.headers,
            session=self.session,
            maximum_tries=self.maximum_tries,
            retry_after=self.retry_after,
        )

    def licenses(self) -> Dict[str, Any]:
        url = f"{self.url}/vocabularies/licences"
        return processing.ApiResponse.from_request(
            "get",
            url,
            headers=self.headers,
            session=self.session,
            maximum_tries=self.maximum_tries,
            retry_after=self.retry_after,
        ).json
