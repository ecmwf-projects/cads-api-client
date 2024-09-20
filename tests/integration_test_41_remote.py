import os
import pathlib
import uuid

import pytest
from requests import HTTPError

from cads_api_client import ApiClient, Remote


@pytest.fixture
def remote(api_anon_client: ApiClient) -> Remote:
    return api_anon_client.submit("test-adaptor-dummy", size=1)


def test_remote_delete(remote: Remote) -> None:
    result = remote.delete()
    assert result["status"] == "dismissed"

    with pytest.raises(HTTPError, match="404 Client Error"):
        remote.status


def test_remote_download(remote: Remote, tmp_path: pathlib.Path) -> None:
    target = str(tmp_path / "dummy.grib")
    result = remote.download(target=target)
    assert result == target
    assert os.path.getsize(result) == 1


def test_remote_request_uid(remote: Remote) -> None:
    assert uuid.UUID(remote.request_uid)


def test_remote_status(remote: Remote) -> None:
    assert remote.status in ("accepted", "running", "successful")
