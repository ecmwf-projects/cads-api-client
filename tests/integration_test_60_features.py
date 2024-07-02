import os
from pathlib import Path
from typing import Any

import pytest

from cads_api_client import ApiClient


def test_features_url_cds_adaptor_area_selection(
    api_client: ApiClient,
    tmp_path: Path,
) -> None:
    collection_id = "test-adaptor-url"
    request: dict[str, Any] = {
        "variable": "grid_point_altitude",
        "reference_dataset": "cru",
        "version": "2.1",
        "retry_options": {"maximum_tries": 0},
    }

    result_bigger = api_client.retrieve(
        collection_id,
        **request,
        target=str(tmp_path / "bigger.zip"),
    )
    result_smaller = api_client.retrieve(
        collection_id,
        **request,
        target=str(tmp_path / "smaller.zip"),
        area=[50, 0, 40, 10],
    )
    assert os.path.getsize(result_bigger) > os.path.getsize(result_smaller)


@pytest.mark.parametrize(
    "format,expected_extension",
    [
        ("grib", ".grib"),
        ("netcdf", ".nc"),
    ],
)
def test_features_mars_cds_adaptor_format(
    api_client: ApiClient,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    request_year: str,
    format: str,
    expected_extension: str,
) -> None:
    monkeypatch.chdir(tmp_path)

    collection_id = "test-adaptor-mars"
    request: dict[str, Any] = {
        "product_type": "reanalysis",
        "variable": "2m_temperature",
        "year": request_year,
        "month": "01",
        "day": "02",
        "time": "00:00",
        "target": None,
        "retry_options": {"maximum_tries": 0},
    }

    result = api_client.retrieve(
        collection_id,
        **request,
        format=format,
    )

    _, actual_extension = os.path.splitext(result)
    assert actual_extension == expected_extension
    assert os.path.getsize(result)
