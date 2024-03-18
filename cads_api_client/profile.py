from typing import Any, Dict

from . import processing


class Profile:
    supported_api_version = "v1"

    def __init__(self, url: str, headers: Dict[str, Any] = {}) -> None:
        self.url = f"{url}/{self.supported_api_version}"
        self.headers = headers

    def profile(self) -> Dict[str, Any]:
        url = f"{self.url}/account"
        response = processing.ApiResponse.from_request("get", url, headers=self.headers)
        return response.json

    def accept_licence(self, licence_id: str, revision: int) -> Dict[str, Any]:
        url = f"{self.url}/account/licences/{licence_id}"
        response = processing.ApiResponse.from_request(
            "put", url, headers=self.headers, json={"revision": revision}
        )
        return response.json

    def accepted_licences(self) -> Dict[str, Any]:
        url = f"{self.url}/account/licences"
        response = processing.ApiResponse.from_request("get", url, headers=self.headers)
        return response.json
