from __future__ import annotations

import functools
import warnings
from typing import Any, overload

import cdsapi
import requests

from . import api_client, processing


class LegacyApiClient(cdsapi.Client):  # type: ignore[misc]
    def __new__(cls, *args: Any, **kwargs: Any) -> Any:
        instantiated: LegacyApiClient = super().__new__(cls)
        return instantiated

    def _initialize_session(self, session: requests.Session) -> requests.Session:
        return session

    @classmethod
    def raise_not_implemented_error(self) -> None:
        raise NotImplementedError(
            "This is a beta version. This functionality has not been implemented yet."
        )

    @functools.cached_property
    def client(self) -> api_client.ApiClient:
        warnings.warn(
            "This is a beta version."
            " `url`, `key`, and `session` are the only legacy parameters currently implemented."
            " Any other parameter is silently ignored.",
            UserWarning,
        )
        return api_client.ApiClient(url=self.url, key=self.key, session=self.session)

    @overload
    def retrieve(self, name: str, request: dict[str, Any], target: str) -> str:
        ...

    @overload
    def retrieve(
        self, name: str, request: dict[str, Any], target: None
    ) -> processing.Remote:
        ...

    def retrieve(
        self, name: str, request: dict[str, Any], target: str | None = None
    ) -> str | processing.Remote:
        collection = self.client.collection(name)
        remote = collection.submit(**request)
        return remote if target is None else remote.download(target)

    def service(self, name, *args, **kwargs):  # type: ignore
        self.raise_not_implemented_error()

    def workflow(self, code, *args, **kwargs):  # type: ignore
        self.raise_not_implemented_error()

    def status(self, context=None):  # type: ignore
        self.raise_not_implemented_error()

    def download(self, results, targets=None):  # type: ignore
        self.raise_not_implemented_error()

    def remote(self, url):  # type: ignore
        self.raise_not_implemented_error()

    def robust(self, call):  # type: ignore
        self.raise_not_implemented_error()
