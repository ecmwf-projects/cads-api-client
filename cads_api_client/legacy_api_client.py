from __future__ import annotations

import functools
import warnings
from typing import Any, Callable, overload

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
        instantiated: LegacyApiClient = super(cdsapi.api.Client, cls).__new__(cls)
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
        self.sleep_max = kwargs.pop("sleep_max", 120)
        self.client = api_client.ApiClient(
            url=self.url,
            key=self.key,
            session=self.session,
            sleep_max=self.sleep_max,
        )

        self.timeout = kwargs.pop("timeout", 60)
        self.retry_max = kwargs.pop("retry_max", 500)
        self.retry_options = {
            "maximum_tries": self.retry_max,
            "retry_after": self.sleep_max,
        }

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
    def retrieve(self, name: str, request: dict[str, Any], target: str) -> str: ...

    @overload
    def retrieve(
        self, name: str, request: dict[str, Any], target: None = ...
    ) -> processing.Results: ...

    def retrieve(
        self, name: str, request: dict[str, Any], target: str | None = None
    ) -> str | processing.Results:
        result = self.client.submit_and_wait_on_result(
            collection_id=name,
            retry_options=self.retry_options,
            **request,
        )
        partial_download: Callable[..., str] = functools.partial(
            result.download,
            timeout=self.timeout,
            retry_options=self.retry_options,
        )
        result.download = partial_download  # type: ignore[method-assign]
        return result if target is None else result.download(target)

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
