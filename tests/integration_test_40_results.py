import os
import pathlib

import pytest

from cads_api_client import ApiClient, Results


@pytest.fixture
def results(api_anon_client: ApiClient) -> Results:
    return api_anon_client.submit_and_wait_on_results("test-adaptor-dummy", size=1)


def test_results_asset(results: Results) -> None:
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


@pytest.mark.parametrize("progress", [True, False])
def test_results_progress(
    api_root_url: str,
    api_anon_key: str,
    tmp_path: pathlib.Path,
    progress: bool,
    capsys: pytest.CaptureFixture[str],
) -> None:
    with capsys.disabled():
        client = ApiClient(
            url=api_root_url, key=api_anon_key, progress=progress, maximum_tries=0
        )
        submitted = client.submit("test-adaptor-dummy")
    submitted.download(target=str(tmp_path / "test.grib"))
    captured = capsys.readouterr()
    assert captured.err if progress else not captured.err
