
import logging
from typing import Any, Dict, List, Optional, TypeVar

import attrs

from cads_api_client.api_response import ApiResponse
from cads_api_client.jobs import Job

T_ApiResponse = TypeVar("T_ApiResponse", bound="ApiResponse")

logger = logging.Logger(__name__)


# TODO as iterator
@attrs.define
class ProcessList(ApiResponse):
    def process_ids(self) -> List[str]:
        return [proc["id"] for proc in self.json["processes"]]

    def next(self) -> Optional[ApiResponse]:
        return self.from_rel_href(rel="next")

    def prev(self) -> Optional[ApiResponse]:
        return self.from_rel_href(rel="prev")

    def __repr__(self):
        # links = {l["rel"]: l.get("href") for l in self.json["links"]}
        processes = '\n'.join([f"- {p['id']} (v{p['version']})" for p in self.json["processes"]])
        return processes


@attrs.define
class Process(ApiResponse):
    headers: Dict[str, Any] = {}

    @property
    def id(self) -> str:
        process_id = self.json["id"]
        assert isinstance(process_id, str)
        return process_id

    def valid_values(self, request: Dict[str, Any] = {}) -> Dict[str, Any]:
        url = f"{self.response.request.url}/constraints"
        response = ApiResponse.from_request("post", url, json={"inputs": request})
        response.response.raise_for_status()
        return response.json

    def execute(
        self,
        inputs: Dict[str, Any],
        accepted_licences: List[Dict[str, Any]] = [],
        **kwargs: Any,
    ) -> Job:
        assert "json" not in kwargs
        url = f"{self.response.request.url}/execute"
        json = {"inputs": inputs, "acceptedLicences": accepted_licences}
        return Job.from_request(
            "post", url, json=json, headers=self.headers, **kwargs
        )



