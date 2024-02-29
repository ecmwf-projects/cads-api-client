from __future__ import annotations

import warnings
from typing import Any, overload

import cdsapi.api
import requests

from . import api_client, processing

LEGACY_KWARGS = [
    "quiet",
    "debug",
    "verify",
    "timeout",
    "progress",
    "full_stack",
    "delete",
    "retry_max",
    "sleep_max",
    "wait_until_complete",
    "info_callback",
    "warning_callback",
    "error_callback",
    "debug_callback",
    "metadata",
    "forget",
    "session",
]


class LegacyApiClient(cdsapi.api.Client):  # type: ignore[misc]
    def __new__(cls, *args: Any, **kwargs: Any) -> LegacyApiClient:
        instantiated: LegacyApiClient = super().__new__(cls)
        return instantiated

    def __init__(
        self,
        url: str | None = None,
        key: str | None = None,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        kwargs.update(zip(LEGACY_KWARGS, args))

        self.url, self.key, _ = cdsapi.api.get_url_key_verify(url, key, None)
        self.session = kwargs.pop("session", requests.Session())
        self.client = api_client.ApiClient(
            url=self.url, key=self.key, session=self.session
        )

        if kwargs:
            warnings.warn(
                "This is a beta version."
                f" The following parameters have not been implemented yet: {kwargs}.",
                UserWarning,
            )

    @classmethod
    def raise_not_implemented_error(self) -> None:
        raise NotImplementedError(
            "This is a beta version. This functionality has not been implemented yet."
        )

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
        if target is None:
            collection = self.client.collection(name)
            return collection.submit(**request)
        return self.client.retrieve(name, target, **request)

    def service(self, name, *args, **kwargs):  # type: ignore
        self.raise_not_implemented_error()

    def workflow(self, code, *args, **kwargs):  # type: ignore
        self.raise_not_implemented_error()

    def status(self, context=None):  # type: ignore
        self.raise_not_implemented_error()

    def info(self, *args, **kwargs):  # type: ignore
        self.raise_not_implemented_error()

    def warning(self, *args, **kwargs):  # type: ignore
        self.raise_not_implemented_error()

    def error(self, *args, **kwargs):  # type: ignore
        self.raise_not_implemented_error()

    def debug(self, *args, **kwargs):  # type: ignore
        self.raise_not_implemented_error()

    def download(self, results, targets=None):  # type: ignore
        self.raise_not_implemented_error()

    def remote(self, url):  # type: ignore
        self.raise_not_implemented_error()

    def robust(self, call):  # type: ignore
        self.raise_not_implemented_error()
