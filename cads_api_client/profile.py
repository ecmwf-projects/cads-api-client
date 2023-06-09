from typing import Any, Dict

from cads_api_client.api_response import ApiResponse


class Profile:
    supported_api_version = "v1"

    def __init__(self, url: str, headers: Dict[str, Any] = {}) -> None:
        self.url = f"{url}/{self.supported_api_version}"
        self.headers = headers

    def profile(self) -> Dict[str, Any]:
        url = f"{self.url}/account"
        response = ApiResponse.from_request("get", url, headers=self.headers)
        return response.json

    def accept_licence(self, licence_id: str) -> Dict[str, Any]:
        url = f"{self.url}/account/licences/{licence_id}"
        response = ApiResponse.from_request("put", url, headers=self.headers)
        return response.json

    def accepted_licences(self) -> Dict[str, Any]:
        url = f"{self.url}/account/licences"
        response = ApiResponse.from_request("get", url, headers=self.headers)
        return response.json
