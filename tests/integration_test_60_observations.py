from pathlib import Path
from typing import Any

import pytest

from cads_api_client import ApiClient

OBS_PARAMS: dict[str, dict[str, Any]] = {
    "insitu-observations-woudc-ozone-total-column-and-profiles": {
        "variable": ["air_temperature"],
        "observation_type": "vertical_profile",
    },
    "insitu-observations-igra-baseline-network": {
        "variable": ["air_temperature"],
        "archive": ["global_radiosonde_archive"],
    },
    "insitu-observations-gnss": {
        "network_type": "igs_r3",
        "variable": [
            "precipitable_water_column",
            "precipitable_water_column_total_uncertainty",
        ],
    },
    "insitu-observations-gruan-reference-network": {
        "variable": ["air_temperature"],
    },
    "insitu-observations-near-surface-temperature-us-climate-reference-network": {
        "time_aggregation": "daily",
        "variable": [
            "maximum_air_temperature",
            "maximum_air_temperature_negative_total_uncertainty",
            "maximum_air_temperature_positive_total_uncertainty",
        ],
    },
}


@pytest.mark.parametrize("collection_id", list(OBS_PARAMS))
def test_collection_retrieve_with_observations_adaptor(
    api_root_url: str,
    api_key: str,
    tmp_path: Path,
    collection_id: str,
) -> None:
    client = ApiClient(url=api_root_url, key=api_key)
    target_path = tmp_path / "obs-test-result.nc"
    common_params: dict[str, Any] = dict(
        year="2014",
        month="01",
        day="01",
        target=str(target_path),
        retry_options={"maximum_tries": 0},
        format="netCDF",
    )
    result = client.retrieve(
        collection_id,
        **common_params,
        **OBS_PARAMS[collection_id],
    )

    assert result == str(target_path)
    assert target_path.exists()
    assert target_path.stat().st_size
