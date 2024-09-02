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
    sleep_max: int
    cleanup: bool
    force_exact_url: bool = False

    def __attrs_post_init__(self) -> None:
        if not self.force_exact_url:
            self.url += f"/{config.SUPPORTED_API_VERSION}"

    def profile(self) -> dict[str, Any]:
        url = f"{self.url}/account"
        response = processing.ApiResponse.from_request(
            "get",
            url,
            headers=self.headers,
            session=self.session,
            retry_options=self.retry_options,
            sleep_max=self.sleep_max,
            cleanup=self.cleanup,
        )
        return response.json

    def accept_licence(self, licence_id: str, revision: int) -> dict[str, Any]:
        url = f"{self.url}/account/licences/{licence_id}"
        response = processing.ApiResponse.from_request(
            "put",
            url,
            json={"revision": revision},
            headers=self.headers,
            session=self.session,
            retry_options=self.retry_options,
            sleep_max=self.sleep_max,
            cleanup=self.cleanup,
        )
        return response.json

    def accepted_licences(self) -> dict[str, Any]:
        url = f"{self.url}/account/licences"
        response = processing.ApiResponse.from_request(
            "get",
            url,
            headers=self.headers,
            session=self.session,
            retry_options=self.retry_options,
            sleep_max=self.sleep_max,
            cleanup=self.cleanup,
        )
        return response.json

    def check_authentication(self) -> dict[str, Any]:
        url = f"{self.url}/account/verification/pat"
        response = processing.ApiResponse.from_request(
            "post",
            url,
            headers=self.headers,
            session=self.session,
            retry_options=self.retry_options,
            sleep_max=self.sleep_max,
            cleanup=self.cleanup,
        )
        return response.json
