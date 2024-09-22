from __future__ import annotations

import functools
import warnings
from typing import Any

import attrs
import multiurl.base
import requests

import cads_api_client

from . import catalogue, config, processing, profile


@attrs.define(slots=False)
class ApiClient:
    """A client to interact with the CADS API.

    Attributes
    ----------
    url: str | None
        API URL. If None, infer from CADS_API_URL or CADS_API_RC.
    key: str | None
        API Key. If None, infer from CADS_API_KEY or CADS_API_RC.
    verify: bool | None
        Whether to verify the TLS certificate at the remote end.
        If None, infer from CADS_API_VERIFY or CADS_API_RC.
    timeout: float | tuple
        How many seconds to wait for the server to send data, as a float, or a (connect, read) tuple.
    progress: bool
        Whether to display the progress bar during download.
    cleanup: bool
        Whether to delete requests after completion.
    sleep_max: float
        Maximum time to wait (in seconds) while checking for a status change.
    retry_after: float
        Time to wait (in seconds) between retries.
    maximum_tries: int
        Maximum number of retries.
    session: requests.Session
        Requests session.
    """

    url: str | None = None
    key: str | None = None
    verify: bool | None = None
    timeout: float | tuple[float, float] = 60
    progress: bool = True
    cleanup: bool = False
    sleep_max: float = 120
    retry_after: float = 120
    maximum_tries: int = 500
    session: requests.Session = attrs.field(factory=requests.Session)

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
        dict[str, Any]
            Content of the response.
        """
        return self._profile_api.accept_licence(licence_id, revision=revision)

    def apply_constraints(self, collection_id: str, **request: Any) -> dict[str, Any]:
        """Apply constraints to a request.

        Parameters
        ----------
        collection_id: str
            Collection ID (e.g., ``"reanalysis-era5-pressure-levels"``).
        **request: Any
            Request parameters.

        Returns
        -------
        dict[str, Any]
            Dictionary of valid values.
        """
        return self.get_process(collection_id).apply_constraints(**request)

    def check_authentication(self) -> dict[str, Any]:
        """Verify authentication.

        Returns
        -------
        dict[str, Any]
            Content of the response.

        Raises
        ------
        requests.HTTPError
            If the authentication fails.
        """
        return self._profile_api.check_authentication()

    def download_results(self, request_uid: str, target: str | None = None) -> str:
        """Download the results of a job.

        Parameters
        ----------
        request_uid: str
            Request UID
        target: str | None
            Target path. If None, download to the working directory.

        Returns
        -------
        str
            Path to the retrieved file.
        """
        return self.get_remote(request_uid).download(target)

    def estimate_costs(self, collection_id: str, **request: Any) -> dict[str, Any]:
        """Estimate costs of a request.

        Parameters
        ----------
        collection_id: str
            Collection ID (e.g., ``"reanalysis-era5-pressure-levels"``).
        **request: Any
            Request parameters.

        Returns
        -------
        dict[str, Any]
            Dictionary of estimated costs.
        """
        return self.get_process(collection_id).estimate_costs(**request)

    def get_collection(self, collection_id: str) -> cads_api_client.Collection:
        """Retrieve a catalogue collection.

        Parameters
        ----------
        collection_id: str
            Collection ID (e.g., ``"reanalysis-era5-pressure-levels"``).

        Returns
        -------
        cads_api_client.Collection
        """
        return self._catalogue_api.get_collection(collection_id)

    def get_process(self, collection_id: str) -> processing.Process:
        """
        Retrieve a process.

        Parameters
        ----------
        collection_id: str
            Collection ID (e.g., ``"reanalysis-era5-pressure-levels"``).

        Returns
        -------
        processing.Process
        """
        return self._retrieve_api.get_process(collection_id)

    def get_remote(self, request_uid: str) -> cads_api_client.Remote:
        """
        Retrieve the remote object of a request.

        Parameters
        ----------
        request_uid: str
            Request UID

        Returns
        -------
        cads_api_client.Remote
        """
        return self._retrieve_api.get_job(request_uid).make_remote()

    def get_results(self, request_uid: str) -> cads_api_client.Results:
        """
        Retrieve the results of a request.

        Parameters
        ----------
        request_uid: str
            Request UID

        Returns
        -------
        cads_api_client.Results
        """
        return self.get_remote(request_uid).make_results()

    def retrieve(
        self,
        collection_id: str,
        target: str | None = None,
        **request: Any,
    ) -> str:
        """Submit of a request and retrieve the results.

        Parameters
        ----------
        collection_id: str
            Collection ID (e.g., ``"reanalysis-era5-pressure-levels"``).
        target: str | None
            Target path. If None, download to the working directory.
        **request: Any
            Request parameters.

        Returns
        -------
        str
            Path to the retrieved file.
        """
        return self.submit(collection_id, **request).download(target)

    def submit(self, collection_id: str, **request: Any) -> cads_api_client.Remote:
        """Submit of a request.

        Parameters
        ----------
        collection_id: str
            Collection ID (e.g., ``"reanalysis-era5-pressure-levels"``).
        **request: Any
            Request parameters.

        Returns
        -------
        cads_api_client.Remote
        """
        return self._retrieve_api.submit(collection_id, **request)

    def submit_and_wait_on_results(
        self, collection_id: str, **request: Any
    ) -> cads_api_client.Results:
        """Submit a request and wait for the results to be ready.

        Parameters
        ----------
        collection_id: str
            Collection ID (e.g., ``"reanalysis-era5-pressure-levels"``).
        **request: Any
            Request parameters.

        Returns
        -------
        cads_api_client.Results
        """
        return self._retrieve_api.submit(collection_id, **request).make_results()

    @property
    def accepted_licences(self) -> list[dict[str, Any]]:
        """Accepted licences.

        Returns
        -------
        list[dict[str, Any]]
            List of dictionaries with license information.
        """
        licences: list[dict[str, Any]]
        licences = self._profile_api.accepted_licences.get("licences", [])
        return licences

    @property
    def collections(self) -> cads_api_client.Collections:
        """Catalogue collections.

        Returns
        -------
        cads_api_client.Collections
        """
        return self._catalogue_api.collections

    @property
    def jobs(self) -> cads_api_client.Jobs:
        """Submitted jobs.

        Returns
        -------
        cads_api_client.Jobs
        """
        return self._retrieve_api.jobs

    @property
    def licences(self) -> list[dict[str, Any]]:
        """Licences.

        Returns
        -------
        list[dict[str, Any]]
            List of dictionaries with license information.
        """
        licences: list[dict[str, Any]]
        licences = self._catalogue_api.licenses.get("licences", [])
        return licences

    @property
    def processes(self) -> processing.Processes:
        """Available processes.

        Returns
        -------
        processing.Processes
        """
        return self._retrieve_api.processes
