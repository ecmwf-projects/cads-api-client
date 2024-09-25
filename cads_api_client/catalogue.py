from __future__ import annotations

import datetime
from typing import Any

import attrs
import requests

import cads_api_client

from . import config
from .processing import ApiResponse, ApiResponsePaginated, RequestKwargs


@attrs.define
class Collections(ApiResponsePaginated):
    """A class to interact with catalogue collections."""

    @property
    def collection_ids(self) -> list[str]:
        """List of collection IDs.

        Return
        ------
        list[str]
        """
        return [collection["id"] for collection in self.json["collections"]]


@attrs.define
class Collection(ApiResponse):
    """A class to interact with a catalogue collection."""

    @property
    def _temporal_interval(self) -> tuple[str, str]:
        begin, end = map(str, self.json["extent"]["temporal"]["interval"][0])
        return (begin, end)

    @property
    def begin_datetime(self) -> datetime.datetime:
        """Begin datetime of the collection.

        Returns
        -------
        datetime.datetime
        """
        return datetime.datetime.fromisoformat(
            self._temporal_interval[0].replace("Z", "+00:00")
        )

    @property
    def end_datetime(self) -> datetime.datetime:
        """End datetime of the collection.

        Returns
        -------
        datetime.datetime
        """
        return datetime.datetime.fromisoformat(
            self._temporal_interval[1].replace("Z", "+00:00")
        )

    @property
    def id(self) -> str:
        """Collection ID.

        Returns
        -------
        str
        """
        return str(self.json["id"])

    @property
    def process(self) -> cads_api_client.Process:
        """
        Collection process.

        Returns
        -------
        cads_api_client.Process
        """
        url = self._get_link_href(rel="retrieve")
        return cads_api_client.Process.from_request("get", url, **self._request_kwargs)

    def submit(self, **request: Any) -> cads_api_client.Remote:
        """Submit a request.

        Parameters
        ----------
        **request: Any
            Request parameters.

        Returns
        -------
        cads_api_client.Remote
        """
        return self.process.submit(**request)


@attrs.define(slots=False)
class Catalogue:
    url: str
    headers: dict[str, Any]
    session: requests.Session
    retry_options: dict[str, Any]
    request_options: dict[str, Any]
    download_options: dict[str, Any]
    sleep_max: float
    cleanup: bool
    force_exact_url: bool = False

    def __attrs_post_init__(self) -> None:
        if not self.force_exact_url:
            self.url += f"/{config.SUPPORTED_API_VERSION}"

    @property
    def _request_kwargs(self) -> RequestKwargs:
        return RequestKwargs(
            headers=self.headers,
            session=self.session,
            retry_options=self.retry_options,
            request_options=self.request_options,
            download_options=self.download_options,
            sleep_max=self.sleep_max,
            cleanup=self.cleanup,
        )

    def get_collections(self, **params: Any) -> Collections:
        url = f"{self.url}/datasets"
        return Collections.from_request(
            "get", url, params=params, **self._request_kwargs
        )

    def get_collection(self, collection_id: str) -> Collection:
        url = f"{self.url}/collections/{collection_id}"
        return Collection.from_request("get", url, **self._request_kwargs)

    def get_licenses(self, **params: Any) -> dict[str, Any]:
        url = f"{self.url}/vocabularies/licences"
        response = ApiResponse.from_request(
            "get", url, params=params, **self._request_kwargs
        )
        return response.json

    @property
    def messages(self) -> ApiResponse:
        url = f"{self.url}/messages"
        return ApiResponse.from_request(
            "get", url, log_messages=False, **self._request_kwargs
        )
