import os
import pathlib

import pytest

from cads_api_client import ApiClient, Results


@pytest.fixture
def results(api_anon_client: ApiClient) -> Results:
    return api_anon_client.submit_and_wait_on_results("test-adaptor-dummy", size=1)


def test_results_assert(results: Results) -> None:
    assert results.asset["type"] == "application/x-grib"
    assert results.asset["file:size"] == 1


@pytest.mark.parametrize("target", ("dummy.grib", None))
def test_results_download(
    monkeypatch: pytest.MonkeyPatch,
    results: Results,
    tmp_path: pathlib.Path,
    target: str | None,
) -> None:
    monkeypatch.chdir(tmp_path)
    actual_target = results.download(target=target)
    assert (actual_target != target) if target is None else (actual_target == target)
    assert os.path.getsize(actual_target) == 1
