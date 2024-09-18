from __future__ import annotations

import functools
import warnings
from typing import Any

import attrs
import multiurl.base
import requests

from . import catalogue, config, processing, profile


@attrs.define(slots=False)
class ApiClient:
    """A client to interact with the CADS API.

    Parameters
    ----------
    url: str or None, default=None
        API URL. If None, infer from CADS_API_URL or CADS_API_RC.
    key: str or None, default=None
        API Key. If None, infer from CADS_API_KEY or CADS_API_RC.
    verify: bool or None, default=None
        Whether to verify the TLS certificate at the remote end.
        If None, infer from CADS_API_VERIFY or CADS_API_RC.
    timeout: float or tuple, default=60
        How many seconds to wait for the server to send data, as a float, or a (connect, read) tuple.
    progress: bool, default=True
        Whether to display the progress bar during download.
    cleanup: bool, default=False
        Whether to delete requests after completion.
    sleep_max: float, default=120
        Maximum time to wait (in seconds) while checking for a status change.
    retry_after: float, default=120
        Time to wait (in seconds) between retries.
    maximum_tries: int, default=500
        Maximum number of retries.
    session: requests.Session
        Requests session.
    """

    url: str | None = None
    """API URL. If None, infer from CADS_API_URL or CADS_API_RC."""
    key: str | None = None
    """API Key. If None, infer from CADS_API_KEY or CADS_API_RC."""
    verify: bool | None = None
    """Whether to verify the TLS certificate at the remote end.
    If None, infer from CADS_API_VERIFY or CADS_API_RC."""
    timeout: float | tuple[float, float] = 60
    """How many seconds to wait for the server to send data, as a float, or a (connect, read) tuple."""
    progress: bool = True
    """Whether to display the progress bar during download."""
    cleanup: bool = False
    """Whether to delete requests after completion."""
    sleep_max: float = 120
    """Maximum time to wait (in seconds) while checking for a status change."""
    retry_after: float = 120
    """Time to wait (in seconds) between retries."""
    maximum_tries: int = 500
    """Maximum number of retries."""
    session: requests.Session = attrs.field(factory=requests.Session)
    """Requests session."""

    def __attrs_post_init__(self) -> None:
        if self.url is None:
            self.url = str(config.get_config("url"))

        if self.key is None:
            try:
                self.key = str(config.get_config("key"))
            except (KeyError, FileNotFoundError):
                warnings.warn("The API key is missing", UserWarning)

        if self.verify is None:
            try:
                self.verify = config.strtobool(str(config.get_config("verify")))
            except (KeyError, FileNotFoundError):
                self.verify = True

    def _get_headers(self, key_is_mandatory: bool = True) -> dict[str, str]:
        if self.key is None:
            if key_is_mandatory:
                raise ValueError("The API key is needed to access this resource")
            return {}
        return {"PRIVATE-TOKEN": self.key}

    @property
    def _retry_options(self) -> dict[str, Any]:
        return {
            "maximum_tries": self.maximum_tries,
            "retry_after": self.retry_after,
        }

    @property
    def _download_options(self) -> dict[str, Any]:
        progress_bar = (
            multiurl.base.progress_bar if self.progress else multiurl.base.NoBar
        )
        return {
            "progress_bar": progress_bar,
        }

    @property
    def _request_options(self) -> dict[str, Any]:
        return {
            "timeout": self.timeout,
            "verify": self.verify,
        }

    def _get_request_kwargs(
        self, mandatory_key: bool = True
    ) -> processing.RequestKwargs:
        return processing.RequestKwargs(
            headers=self._get_headers(key_is_mandatory=mandatory_key),
            session=self.session,
            retry_options=self._retry_options,
            request_options=self._request_options,
            download_options=self._download_options,
            sleep_max=self.sleep_max,
            cleanup=self.cleanup,
        )

    @functools.cached_property
    def _catalogue_api(self) -> catalogue.Catalogue:
        return catalogue.Catalogue(
            f"{self.url}/catalogue",
            **self._get_request_kwargs(mandatory_key=False),
        )

    @functools.cached_property
    def _retrieve_api(self) -> processing.Processing:
        return processing.Processing(
            f"{self.url}/retrieve", **self._get_request_kwargs()
        )

    @functools.cached_property
    def _profile_api(self) -> profile.Profile:
        return profile.Profile(f"{self.url}/profiles", **self._get_request_kwargs())

    @property
    def accepted_licences(self) -> list[dict[str, Any]]:
        """List all accepted licences.

        Returns
        -------
        list
            List of dictionaries with license information.
        """
        licences: list[dict[str, Any]]
        licences = self._profile_api.accepted_licences.get("licences", [])
        return licences

    @property
    def licences(self) -> list[dict[str, Any]]:
        """List all licences.

        Returns
        -------
        list
            List of dictionaries with license information.
        """
        licences: list[dict[str, Any]]
        licences = self._catalogue_api.licenses.get("licences", [])
        return licences

    def accept_licence(self, licence_id: str, revision: int) -> dict[str, Any]:
        """Accept a licence.

        Parameters
        ----------
        licence_id: str
            Licence ID.
        revision: int
            Licence revision number.

        Returns
        -------
        dict
            Content of the response.
        """
        return self._profile_api.accept_licence(licence_id, revision=revision)

    def check_authentication(self) -> dict[str, Any]:
        """Verify API authentication.

        Returns
        -------
        dict
            Content of the response.

        Raises
        ------
        requests.HTTPError
            If the authentication fails.
        """
        return self._profile_api.check_authentication()

    def retrieve(
        self,
        collection_id: str,
        target: str | None = None,
        **request: Any,
    ) -> str:
        """Submit a request and retrieve the result.

        Parameters
        ----------
        collection_id: str
            Collection ID (e.g., reanalysis-era5-pressure-levels).
        target: str or None, default=None
            Target path. If None, download to the working directory.
        **request: dict
            Request parameters.

        Returns
        -------
        str
            Path to the retrieved file.
        """
        result = self.submit_and_wait_on_result(collection_id, **request)
        return result.download(target)

    def submit(self, collection_id: str, **request: Any) -> processing.Remote:
        """Submit a request.

        Parameters
        ----------
        collection_id: str
            Collection ID (e.g., reanalysis-era5-pressure-levels).
        **request: dict
            Request parameters.

        Returns
        -------
        processing.Remote
        """
        return self._retrieve_api.submit(collection_id, **request)

    def submit_and_wait_on_result(
        self, collection_id: str, **request: Any
    ) -> processing.Results:
        """Submit a request and wait for the result to be ready.

        Parameters
        ----------
        collection_id: str
            Collection ID (e.g., reanalysis-era5-pressure-levels).
        **request: dict
            Request parameters.

        Returns
        -------
        processing.Results
        """
        return self._retrieve_api.submit_and_wait_on_result(collection_id, **request)

    def collections(self, **params: dict[str, Any]) -> catalogue.Collections:
        return self._catalogue_api.collections(params=params)

    def collection(self, collection_id: str) -> catalogue.Collection:
        return self._catalogue_api.collection(collection_id)

    def processes(self, **params: dict[str, Any]) -> processing.ProcessList:
        return self._retrieve_api.processes(params=params)

    def process(self, process_id: str) -> processing.Process:
        return self._retrieve_api.process(process_id=process_id)

    def get_requests(self, **params: dict[str, Any]) -> processing.JobList:
        return self._retrieve_api.jobs(params=params)

    def get_request(self, request_uid: str) -> processing.StatusInfo:
        return self._retrieve_api.job(request_uid)

    def get_remote(self, request_uid: str) -> processing.Remote:
        request = self.get_request(request_uid=request_uid)
        return request.make_remote()

    def download_result(self, request_uid: str, target: str | None) -> str:
        return self._retrieve_api.download_result(request_uid, target)

    def valid_values(
        self, collection_id: str, request: dict[str, Any]
    ) -> dict[str, Any]:
        process = self._retrieve_api.process(collection_id)
        return process.valid_values(request)
