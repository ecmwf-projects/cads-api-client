import functools
import logging
from typing import Any, Dict, List

from cads_api_client.jobs import Job


logger = logging.Logger(__name__)


class Process:
    def __init__(self, pid, base_url, session, headers):
        self.pid = pid
        self.session = session
        self.headers = headers
        self.base_url = base_url
        self.url = f"{base_url}/retrieve/v1/processes/{pid}"   # TODO from settings
        self._constraints_url = f"{self.url}/constraints"
        self._execute_url = f"{self.url}/execute"

    def __repr__(self):
        return f"Process(pid={self.pid})"

    @functools.cached_property
    def _response(self):
        return self.session.get(self.url)

    def json(self):
        return self._response.json()

    @property
    def id(self) -> str:
        process_id = self.json()["id"]
        assert isinstance(process_id, str)
        return process_id

    def valid_values(self, request: Dict[str, Any] = {}) -> Dict[str, Any]:
        response = self.session.post(self._constraints_url, json={"inputs": request})
        response.raise_for_status()
        return response.json()

    def execute(
        self,
        inputs: Dict[str, Any],
        accepted_licences: List[Dict[str, Any]] = [],
        **kwargs: Any,
    ) -> Job:
        assert "json" not in kwargs
        json = {"inputs": inputs, "acceptedLicences": accepted_licences}
        execute_resp = self.session.post(self._execute_url, json=json, headers=self.headers)
        execute_resp.raise_for_status()
        job_id = execute_resp.json()["id"]
        return Job(job_id=job_id, request=inputs, base_url=self.base_url, session=self.session, headers=self.headers)
