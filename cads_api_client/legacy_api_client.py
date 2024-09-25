from __future__ import annotations

import collections
import functools
import logging
import typing
import warnings
from types import TracebackType
from typing import Any, Callable, TypeVar, cast, overload

import cdsapi.api
import multiurl
import requests

from . import __version__ as cads_api_client_version
from . import processing
from .api_client import ApiClient
from .processing import Remote, Results

LEGACY_KWARGS = [
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

LOGGER = logging.getLogger(__name__)
F = TypeVar("F", bound=Callable[..., Any])


class LoggingContext:
    def __init__(self, logger: logging.Logger, quiet: bool, debug: bool) -> None:
        self.old_level = logger.level
        if quiet:
            logger.setLevel(logging.WARNING)
        else:
            logger.setLevel(logging.DEBUG if debug else logging.INFO)

        self.new_handlers = []
        if not logger.handlers:
            formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
            handler = logging.StreamHandler()
            handler.setFormatter(formatter)
            logger.addHandler(handler)
            self.new_handlers.append(handler)

        self.logger = logger

    def __enter__(self) -> logging.Logger:
        return self.logger

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        self.logger.setLevel(self.old_level)
        for handler in self.new_handlers:
            self.logger.removeHandler(handler)


class LegacyApiClient(cdsapi.api.Client):  # type: ignore[misc]
    def __init__(
        self,
        url: str | None = None,
        key: str | None = None,
        quiet: bool = False,
        debug: bool = False,
        verify: bool | int | None = None,
        timeout: int = 60,
        progress: bool = True,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        kwargs.update(zip(LEGACY_KWARGS, args))
        if wrong_kwargs := set(kwargs) - set(LEGACY_KWARGS):
            raise ValueError(f"Wrong parameters: {wrong_kwargs}.")

        self.url, self.key, verify = cdsapi.api.get_url_key_verify(url, key, verify)
        self.verify = bool(verify)
        self.quiet = quiet
        self._debug = debug
        self.timeout = timeout
        self.progress = progress

        self.sleep_max = kwargs.pop("sleep_max", 120)
        self.wait_until_complete = kwargs.pop("wait_until_complete", True)
        self.delete = kwargs.pop("delete", False)
        self.retry_max = kwargs.pop("retry_max", 500)
        self.session = kwargs.pop("session", requests.Session())
        if kwargs:
            warnings.warn(
                "This is a beta version."
                f" The following parameters have not been implemented yet: {kwargs}.",
                UserWarning,
            )

        self.client = self.logging_decorator(ApiClient)(
            url=self.url,
            key=self.key,
            verify=self.verify,
            sleep_max=self.sleep_max,
            session=self.session,
            cleanup=self.delete,
            maximum_tries=self.retry_max,
            retry_after=self.sleep_max,
            timeout=self.timeout,
            progress=self.progress,
        )
        self.debug(
            "CDSAPI %s",
            {
                "url": self.url,
                "key": self.key,
                "quiet": self.quiet,
                "verify": self.verify,
                "timeout": self.timeout,
                "progress": self.progress,
                "sleep_max": self.sleep_max,
                "retry_max": self.retry_max,
                "delete": self.delete,
                "cads_api_client_version": cads_api_client_version,
            },
        )

    @classmethod
    def raise_not_implemented_error(self) -> None:
        raise NotImplementedError(
            "This is a beta version. This functionality has not been implemented yet."
        )

    def logging_decorator(self, func: F) -> F:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            with LoggingContext(
                logger=processing.LOGGER, quiet=self.quiet, debug=self._debug
            ):
                return func(*args, **kwargs)

        return cast(F, wrapper)

    @overload
    def retrieve(self, name: str, request: dict[str, Any], target: str) -> str: ...

    @overload
    def retrieve(
        self, name: str, request: dict[str, Any], target: None = ...
    ) -> Results: ...

    def retrieve(
        self, name: str, request: dict[str, Any], target: str | None = None
    ) -> str | Remote | Results:
        submitted: Remote | Results
        if self.wait_until_complete:
            submitted = self.logging_decorator(self.client.submit_and_wait_on_results)(
                collection_id=name,
                **request,
            )
        else:
            submitted = self.logging_decorator(self.client.submit)(
                collection_id=name,
                **request,
            )

        # Decorate legacy methods
        submitted.download = self.logging_decorator(submitted.download)  # type: ignore[method-assign]
        submitted.log = self.logging_decorator(submitted.log)  # type: ignore[method-assign]

        return submitted if target is None else submitted.download(target)

    def log(self, *args: Any, **kwargs: Any) -> None:
        with LoggingContext(
            logger=LOGGER, quiet=self.quiet, debug=self._debug
        ) as logger:
            logger.log(*args, **kwargs)

    def info(self, *args: Any, **kwargs: Any) -> None:
        self.log(logging.INFO, *args, **kwargs)

    def warning(self, *args: Any, **kwargs: Any) -> None:
        self.log(logging.WARNING, *args, **kwargs)

    def error(self, *args: Any, **kwargs: Any) -> None:
        self.log(logging.ERROR, *args, **kwargs)

    def debug(self, *args: Any, **kwargs: Any) -> None:
        self.log(logging.DEBUG, *args, **kwargs)

    def service(self, name, *args, **kwargs):  # type: ignore
        self.raise_not_implemented_error()

    def workflow(self, code, *args, **kwargs):  # type: ignore
        self.raise_not_implemented_error()

    def status(self, context: Any = None) -> dict[str, list[str]]:
        status = collections.defaultdict(list)
        for message in self.client._catalogue_api.messages.json.get("messages", []):
            status[message["severity"]].append(message["content"])
        return dict(status)

    @typing.no_type_check
    def _download(self, results, targets=None):
        if isinstance(results, (cdsapi.api.Result, Remote, Results)):
            if targets:
                path = targets.pop(0)
            else:
                path = None
            return results.download(path)

        if isinstance(results, (list, tuple)):
            return [self._download(x, targets) for x in results]

        if isinstance(results, dict):
            if "location" in results and "contentLength" in results:
                reply = dict(
                    location=results["location"],
                    content_length=results["contentLength"],
                    content_type=results.get("contentType"),
                )

                if targets:
                    path = targets.pop(0)
                else:
                    path = None

                return cdsapi.api.Result(self, reply).download(path)

            r = {}
            for v in results.values():
                r[v] = self._download(v, targets)
            return r

        return results

    @typing.no_type_check
    def download(self, results, targets=None):
        if targets:
            # Make a copy
            targets = [t for t in targets]
        return self._download(results, targets)

    def remote(self, url: str) -> cdsapi.api.Result:
        r = requests.head(url)
        reply = dict(
            location=url,
            content_length=r.headers["Content-Length"],
            content_type=r.headers["Content-Type"],
        )
        return cdsapi.api.Result(self, reply)

    def robust(self, call: F) -> F:
        robust: F = multiurl.robust(call, **self.client._retry_options)
        return robust
