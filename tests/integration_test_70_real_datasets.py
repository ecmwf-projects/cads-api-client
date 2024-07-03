"""TODO: Move these tests to a dedicated repository."""

import datetime
import os
from pathlib import Path
from typing import Any

import pytest

from cads_api_client import ApiClient

XFAIL_REASON = "temporary E2E tests"


@pytest.mark.xfail(reason=XFAIL_REASON)
@pytest.mark.parametrize(
    "collection_id,request_params",
    [
        (
            "insitu-observations-woudc-ozone-total-column-and-profiles",
            {
                "variable": ["air_temperature"],
                "observation_type": "vertical_profile",
            },
        ),
        (
            "insitu-observations-igra-baseline-network",
            {
                "variable": ["air_temperature"],
                "archive": ["global_radiosonde_archive"],
            },
        ),
        (
            "insitu-observations-gnss",
            {
                "network_type": "igs_r3",
                "variable": [
                    "precipitable_water_column",
                    "precipitable_water_column_total_uncertainty",
                ],
            },
        ),
        (
            "insitu-observations-gruan-reference-network",
            {
                "variable": ["air_temperature"],
            },
        ),
        (
            "insitu-observations-near-surface-temperature-us-climate-reference-network",
            {
                "time_aggregation": "daily",
                "variable": [
                    "maximum_air_temperature",
                    "maximum_air_temperature_negative_total_uncertainty",
                    "maximum_air_temperature_positive_total_uncertainty",
                ],
            },
        ),
    ],
)
def test_real_datasets_observations(
    api_client: ApiClient,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    collection_id: str,
    request_params: dict[str, Any],
) -> None:
    monkeypatch.chdir(tmp_path)

    common_params: dict[str, Any] = {
        "year": "2014",
        "month": "01",
        "day": "01",
        "format": "netCDF",
        "retry_options": {"maximum_tries": 0},
        "target": None,
    }

    result = api_client.retrieve(
        collection_id,
        **common_params,
        **request_params,
    )

    _, extension = os.path.splitext(result)
    assert extension == ".nc"
    assert os.path.getsize(result)


@pytest.mark.xfail(reason=XFAIL_REASON)
@pytest.mark.parametrize(
    "collection_id,request_params",
    [
        (
            "reanalysis-era5-single-levels",
            {
                "variable": "2m_temperature",
                "product_type": "reanalysis",
                "year": "2020",
                "month": "01",
                "day": "01",
                "time": "00:00",
            },
        ),
        (
            "seasonal-original-single-levels",
            {
                "originating_centre": "ukmo",
                "system": "12",
                "variable": ["2m_dewpoint_temperature"],
                "year": ["2015"],
                "month": ["02"],
                "day": ["09"],
                "leadtime_hour": ["390"],
            },
        ),
        (
            "cams-global-atmospheric-composition-forecasts",
            {
                "variable": [
                    "total_column_dimethyl_sulfide",
                    "total_column_glyoxal",
                ],
                "date": "2024-01-02",
                "time": "00:00",
                "leadtime_hour": "0",
                "type": "forecast",
            },
        ),
    ],
)
@pytest.mark.parametrize(
    "data_format,expected_extension",
    [
        ("grib", ".grib"),
        ("netcdf", ".nc"),
    ],
)
def test_real_datasets_format(
    api_client: ApiClient,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    collection_id: str,
    request_params: dict[str, Any],
    data_format: str,
    expected_extension: str,
) -> None:
    monkeypatch.chdir(tmp_path)

    common_params: dict[str, Any] = {
        "data_format": data_format,
        "_timestamp": datetime.datetime.now().isoformat(),
        "retry_options": {"maximum_tries": 0},
        "target": None,
    }

    result = api_client.retrieve(
        collection_id,
        **common_params,
        **request_params,
    )

    _, actual_extension = os.path.splitext(result)
    assert actual_extension == expected_extension
    assert os.path.getsize(result)
