import functools
import logging
from typing import Any, Dict, List

from cads_api_client.jobs import Job
from cads_api_client.utils import ConnectionObject

logger = logging.Logger(__name__)

from cads_api_client.settings import RETRIEVE_DIR, API_VERSION


class Process(ConnectionObject):
    def __init__(self, pid, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.pid = pid
        self._url = f"{self.base_url}/{RETRIEVE_DIR}/v{API_VERSION}/processes/{pid}"
        self._constraints_url = f"{self._url}/constraints"
        self._execute_url = f"{self._url}/execute"

    def __repr__(self):
        return f"Process(pid={self.pid})"

    @functools.cached_property
    def _response(self):
        return self.session.get(self._url)

    def json(self):
        return self._response.json()

    @property
    def id(self) -> str:
        # process_id = self.json()["id"]
        # assert isinstance(process_id, str)
        # return process_id
        return self.pid

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
        job_id = execute_resp.json()["jobID"]
        return Job(job_id=job_id, request=inputs, response=execute_resp,
                   base_url=self.base_url, session=self.session, api_key=self.api_key)
