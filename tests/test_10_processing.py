import logging

import pytest
import responses
from responses.matchers import json_params_matcher

import cads_api_client

COLLECTION_ID = "reanalysis-era5-pressure-levels"
JOB_RUNNING_ID = "9bfc1362-2832-48e1-a235-359267420bb1"
JOB_SUCCESSFUL_ID = "9bfc1362-2832-48e1-a235-359267420bb2"

CATALOGUE_URL = "http://localhost:8080/api/catalogue"
COLLECTIONS_URL = "http://localhost:8080/api/catalogue/v1/datasets"
COLLECTION_URL = (
    "http://localhost:8080/api/catalogue/v1/collections/reanalysis-era5-pressure-levels"
)
PROCESS_URL = f"http://localhost:8080/api/retrieve/v1/processes/{COLLECTION_ID}"
EXECUTE_URL = f"{PROCESS_URL}/execute"

JOB_RUNNING_URL = f"http://localhost:8080/api/retrieve/v1/jobs/{JOB_RUNNING_ID}"
JOB_SUCCESSFUL_URL = f"http://localhost:8080/api/retrieve/v1/jobs/{JOB_SUCCESSFUL_ID}"

RESULT_RUNNING_URL = (
    f"http://localhost:8080/api/retrieve/v1/jobs/{JOB_RUNNING_ID}/results"
)
RESULT_SUCCESSFUL_URL = (
    f"http://localhost:8080/api/retrieve/v1/jobs/{JOB_SUCCESSFUL_ID}/results"
)

CATALOGUE_JSON = {
    "type": "Catalog",
    "id": "stac-fastapi",
    "links": [
        {"rel": "self", "type": "application/json", "href": f"{CATALOGUE_URL}/v1"},
        {"rel": "root", "type": "application/json", "href": f"{CATALOGUE_URL}/v1"},
        {"rel": "data", "type": "application/json", "href": f"{COLLECTIONS_URL}"},
        {
            "rel": "child",
            "type": "application/json",
            "title": "ERA5 hourly data on pressure levels from 1959 to present",
            "href": f"{COLLECTION_URL}",
        },
    ],
}

COLLECTIONS_JSON = {
    "collections": [
        {
            "type": "Collection",
            "id": f"{COLLECTION_ID}",
            "links": [
                {
                    "rel": "self",
                    "type": "application/json",
                    "href": f"{COLLECTION_URL}",
                },
                {
                    "rel": "parent",
                    "type": "application/json",
                    "href": f"{CATALOGUE_URL}/v1",
                },
                {
                    "rel": "root",
                    "type": "application/json",
                    "href": f"{CATALOGUE_URL}/v1",
                },
            ],
        },
    ],
    "links": [
        {"rel": "root", "type": "application/json", "href": f"{CATALOGUE_URL}/v1"},
        {"rel": "parent", "type": "application/json", "href": f"{CATALOGUE_URL}/v1"},
        {"rel": "self", "type": "application/json", "href": f"{COLLECTIONS_URL}"},
    ],
}

COLLECTION_JSON = {
    "type": "Collection",
    "id": COLLECTION_ID,
    "links": [
        {
            "rel": "self",
            "type": "application/json",
            "href": "http://localhost:8080/api/catalogue/v1/collections/reanalysis-era5-pressure-levels",
        },
        {
            "rel": "parent",
            "type": "application/json",
            "href": "http://localhost:8080/api/catalogue/v1/",
        },
        {
            "rel": "root",
            "type": "application/json",
            "href": "http://localhost:8080/api/catalogue/v1/",
        },
        {
            "rel": "retrieve",
            "href": "http://localhost:8080/api/retrieve/v1/processes/reanalysis-era5-pressure-levels",
            "type": "application/json",
        },
        {
            "rel": "related",
            "href": "http://localhost:8080/api/catalogue/v1/collections/reanalysis-era5-single-levels",
        },
    ],
    "tmp:variables": {
        "Temperature": {
            "units": "K",
        }
    },
}

PROCESS_JSON = {
    "id": COLLECTION_ID,
    "links": [
        {"href": COLLECTION_URL, "rel": "self", "type": "application/json"},
        {"rel": "retrieve", "href": PROCESS_URL, "type": "application/json"},
        {
            "href": f"{COLLECTION_URL}/execute",
            "rel": "execute",
            "type": "application/json",
            "title": "process execution",
        },
    ],
    "inputs": {
        "product_type": {
            "title": "Product type",
            "schema": {
                "type": "array",
                "items": {"enum": ["ensemble_mean", "reanalysis"], "type": "string"},
            },
        },
        "variable": {
            "title": "Variable",
            "schema": {
                "type": "array",
                "items": {"enum": ["temperature", "vorticity"], "type": "string"},
            },
        },
        "year": {
            "title": "Year",
            "schema": {"type": "array", "items": {"enum": ["2022"], "type": "string"}},
        },
    },
    "outputs": {
        "download_url": {
            "schema": {"type": "string", "format": "url"},
        }
    },
    "metadata": {
        "datasetMetadata": {
            "messages": [
                {
                    "date": "2023-12-12T13:00:00",
                    "severity": "warning",
                    "content": "This is a warning",
                },
                {
                    "date": "2023-12-12T13:00:00",
                    "severity": "success",
                    "content": "This is a success",
                },
            ]
        }
    },
}

JOB_RUNNING_JSON = {
    "processID": f"{COLLECTION_ID}",
    "type": "process",
    "jobID": f"{JOB_RUNNING_ID}",
    "status": "running",
    "created": "2022-09-02T17:30:48.201213",
    "updated": "2022-09-02T17:30:48.201217",
    "links": [
        {
            "href": f"{COLLECTION_URL}/execute",
            "rel": "self",
            "type": "application/json",
        },
        {
            "href": f"{JOB_RUNNING_URL}",
            "rel": "monitor",
            "type": "application/json",
            "title": "job status info",
        },
    ],
}


JOB_SUCCESSFUL_JSON = {
    "processID": f"{COLLECTION_ID}",
    "type": "process",
    "jobID": f"{JOB_SUCCESSFUL_ID}",
    "status": "successful",
    "created": "2022-09-02T17:30:48.201213",
    "started": "2022-09-02T17:32:43.890617",
    "finished": "2022-09-02T17:32:54.308120",
    "updated": "2022-09-02T17:32:54.308116",
    "links": [
        {"href": f"{JOB_SUCCESSFUL_ID}", "rel": "self", "type": "application/json"},
        {
            "href": f"http://localhost:8080/api/retrieve/v1/jobs/{JOB_SUCCESSFUL_ID}/results",
            "rel": "results",
        },
        {
            "href": f"{JOB_SUCCESSFUL_URL}",
            "rel": "monitor",
            "type": "application/json",
            "title": "job status info",
        },
    ],
}

RESULT_SUCCESSFUL_JSON = {
    "asset": {
        "value": {
            "type": "application/netcdf",
            "href": "./e7d452a747061ab880887d88814bfb0c27593a73cb7736d2dc340852",
            "file:checksum": "e7d452a747061ab880887d88814bfb0c27593a73cb7736d2dc340852",
            "file:size": 8,
            "file:local_path": [
                "/cache-store/",
                "e7d452a747061ab880887d88814bfb0c27593a73cb7736d2dc340852.nc",
            ],
            "xarray:open_kwargs": {},
            "xarray:storage_options": {},
        }
    }
}


RESULT_RUNNING_JSON = {
    "type": "http://www.opengis.net/def/exceptions/ogcapi-processes-1/1.0/result-not-ready",
    "title": "job results not ready",
    "detail": "job 8b7a1f3d-04b1-425d-96f1-f0634d02ee7f results are not yet ready",
    "instance": "http://127.0.0.1:8080/api/retrieve/v1/jobs/8b7a1f3d-04b1-425d-96f1-f0634d02ee7f/results",
}


def responses_add() -> None:
    responses.add(
        responses.GET,
        url=f"{CATALOGUE_URL}/v1/",
        json=CATALOGUE_JSON,
        content_type="application/json",
    )

    responses.add(
        responses.GET,
        url=f"{COLLECTIONS_URL}",
        json=COLLECTIONS_JSON,
        content_type="application/json",
    )

    responses.add(
        responses.GET,
        url=f"{COLLECTION_URL}",
        json=COLLECTION_JSON,
        content_type="application/json",
    )

    responses.add(
        responses.GET,
        url=PROCESS_URL,
        json=PROCESS_JSON,
        content_type="application/json",
    )

    responses.add(
        responses.POST,
        url=EXECUTE_URL,
        json=JOB_SUCCESSFUL_JSON,
        match=[
            json_params_matcher(
                {
                    "inputs": {"variable": "temperature", "year": "2022"},
                }
            )
        ],
        content_type="application/json",
    )

    responses.add(
        responses.GET,
        url=JOB_SUCCESSFUL_URL,
        json=JOB_SUCCESSFUL_JSON,
        content_type="application/json",
    )


@responses.activate
def test_catalogue_collections() -> None:
    responses_add()

    catalogue = cads_api_client.Catalogue(CATALOGUE_URL)

    collections = catalogue.collections()
    assert collections.response.json() == COLLECTIONS_JSON

    collection = catalogue.collection(COLLECTION_ID)
    assert collection.response.json() == COLLECTION_JSON


@responses.activate
def test_submit() -> None:
    responses_add()

    catalogue = cads_api_client.Catalogue(CATALOGUE_URL)
    collection = catalogue.collection(COLLECTION_ID)
    process = collection.retrieve_process()

    assert process.response.json() == PROCESS_JSON

    job = process.execute(inputs={"variable": "temperature", "year": "2022"})
    assert job.response.json() == JOB_SUCCESSFUL_JSON

    remote = collection.submit(variable="temperature", year="2022")
    assert remote.url == JOB_SUCCESSFUL_URL
    assert remote.status == "successful"


@responses.activate
def test_wait_on_result() -> None:
    responses_add()

    catalogue = cads_api_client.Catalogue(CATALOGUE_URL)
    collection = catalogue.collection(COLLECTION_ID)
    remote = collection.submit(variable="temperature", year="2022")
    remote.wait_on_result()


@responses.activate
def test_log_messages(caplog: pytest.LogCaptureFixture) -> None:
    responses_add()

    catalogue = cads_api_client.Catalogue(CATALOGUE_URL)
    collection = catalogue.collection(COLLECTION_ID)

    with caplog.at_level(logging.DEBUG, logger="cads_api_client.processing"):
        process = collection.retrieve_process()
        _ = process.execute(inputs={"variable": "temperature", "year": "2022"})

    assert caplog.record_tuples == [
        ("cads_api_client.processing", 30, "This is a warning"),
        ("cads_api_client.processing", 20, "This is a success"),
    ]
