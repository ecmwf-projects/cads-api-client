from __future__ import annotations

import functools
from typing import Any

import attrs
import requests

from . import catalogue, config, processing, profile


def strtobool(value: str) -> bool:
    if value.lower() in ("y", "yes", "t", "true", "on", "1"):
        return True
    if value.lower() in ("n", "no", "f", "false", "off", "0"):
        return False
    raise ValueError(f"invalid truth value {value!r}")


@attrs.define(slots=False)
class ApiClient:
    url: str | None = None
    key: str | None = None
    verify: bool | None = None
    timeout: int = 60
    cleanup: bool = False
    sleep_max: int = 120
    retry_after: int = 120
    maximum_tries: int = 500
    session: requests.Session = attrs.field(factory=requests.Session)

    def get_url(self) -> str:
        return str(config.get_config("url") if self.url is None else self.url)

    def get_key(self) -> str:
        return str(config.get_config("key") if self.key is None else self.key)

    def get_verify(self) -> bool:
        verify = str(
            config.get_config("verify") if self.verify is None else self.verify
        )
        return strtobool(verify)

    def _get_headers(self, key_is_mandatory: bool = True) -> dict[str, str]:
        try:
            key = self.get_key()
        except KeyError:
            if key_is_mandatory:
                raise ValueError("A valid API key is needed to access this resource")
            return {}
        return {"PRIVATE-TOKEN": key}

    @property
    def _retry_options(self) -> dict[str, Any]:
        return {
            "maximum_tries": self.maximum_tries,
            "retry_after": self.retry_after,
        }

    @property
    def _request_options(self) -> dict[str, Any]:
        return {
            "timeout": self.timeout,
            "verify": self.get_verify(),
        }

    def _get_request_kwargs(
        self, mandatory_key: bool = True
    ) -> processing.RequestKwargs:
        return processing.RequestKwargs(
            headers=self._get_headers(key_is_mandatory=mandatory_key),
            session=self.session,
            retry_options=self._retry_options,
            request_options=self._request_options,
            sleep_max=self.sleep_max,
            cleanup=self.cleanup,
        )

    @functools.cached_property
    def catalogue_api(self) -> catalogue.Catalogue:
        return catalogue.Catalogue(
            f"{self.get_url()}/catalogue",
            **self._get_request_kwargs(mandatory_key=False),
        )

    @functools.cached_property
    def retrieve_api(self) -> processing.Processing:
        return processing.Processing(
            f"{self.get_url()}/retrieve", **self._get_request_kwargs()
        )

    @functools.cached_property
    def profile_api(self) -> profile.Profile:
        return profile.Profile(
            f"{self.get_url()}/profiles", **self._get_request_kwargs()
        )

    def check_authentication(self) -> dict[str, Any]:
        return self.profile_api.check_authentication()

    def collections(self, **params: dict[str, Any]) -> catalogue.Collections:
        return self.catalogue_api.collections(params=params)

    def collection(self, collection_id: str) -> catalogue.Collection:
        return self.catalogue_api.collection(collection_id)

    def processes(self, **params: dict[str, Any]) -> processing.ProcessList:
        return self.retrieve_api.processes(params=params)

    def process(self, process_id: str) -> processing.Process:
        return self.retrieve_api.process(process_id=process_id)

    def submit(self, collection_id: str, **request: Any) -> processing.Remote:
        return self.retrieve_api.submit(collection_id, **request)

    def submit_and_wait_on_result(
        self, collection_id: str, **request: Any
    ) -> processing.Results:
        return self.retrieve_api.submit_and_wait_on_result(collection_id, **request)

    def retrieve(
        self,
        collection_id: str,
        target: str | None = None,
        **request: Any,
    ) -> str:
        result = self.submit_and_wait_on_result(collection_id, **request)
        return result.download(target)

    def get_requests(self, **params: dict[str, Any]) -> processing.JobList:
        return self.retrieve_api.jobs(params=params)

    def get_request(self, request_uid: str) -> processing.StatusInfo:
        return self.retrieve_api.job(request_uid)

    def get_remote(self, request_uid: str) -> processing.Remote:
        request = self.get_request(request_uid=request_uid)
        return request.make_remote()

    def download_result(self, request_uid: str, target: str | None) -> str:
        return self.retrieve_api.download_result(request_uid, target)

    def valid_values(
        self, collection_id: str, request: dict[str, Any]
    ) -> dict[str, Any]:
        process = self.retrieve_api.process(collection_id)
        return process.valid_values(request)

    @property
    def licences(self) -> dict[str, Any]:
        return self.catalogue_api.licenses()

    @property
    def accepted_licences(self) -> dict[str, Any]:
        return self.profile_api.accepted_licences()

    def accept_licence(self, licence_id: str, revision: int) -> dict[str, Any]:
        return self.profile_api.accept_licence(licence_id, revision=revision)
