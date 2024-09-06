from __future__ import annotations

from typing import Any

import attrs
import requests

from . import config, processing


@attrs.define(slots=False)
class Profile:
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

    def get_api_response(
        self, method: str, url: str, **kwargs: Any
    ) -> processing.ApiResponse:
        return processing.ApiResponse.from_request(
            method,
            url,
            **self.request_kwargs,
            **kwargs,
        )

    def profile(self) -> dict[str, Any]:
        url = f"{self.url}/account"
        return self.get_api_response("get", url).json

    def accept_licence(self, licence_id: str, revision: int) -> dict[str, Any]:
        url = f"{self.url}/account/licences/{licence_id}"
        return self.get_api_response("put", url, json={"revision": revision}).json

    def accepted_licences(self) -> dict[str, Any]:
        url = f"{self.url}/account/licences"
        return self.get_api_response("get", url).json

    def check_authentication(self) -> dict[str, Any]:
        url = f"{self.url}/account/verification/pat"
        return self.get_api_response("post", url).json
