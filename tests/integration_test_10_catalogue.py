import pytest
import requests

from cads_api_client import catalogue


@pytest.fixture
def cat(api_root_url: str) -> catalogue.Catalogue:
    return catalogue.Catalogue(
        f"{api_root_url}/catalogue",
        headers={},
        session=requests.Session(),
        retry_options={},
        request_options={},
        download_options={},
        sleep_max=120,
        cleanup=False,
    )


def test_collections(cat: catalogue.Catalogue) -> None:
    res: catalogue.Collections | None = cat.collections()

    assert isinstance(res, catalogue.Collections)
    assert "collections" in res.json
    assert isinstance(res.json["collections"], list)
    assert "links" in res.json
    assert isinstance(res.json["links"], list)

    collection_ids = res.collection_ids()
    while len(collection_ids) != res.json["numberMatched"]:
        res = res.next()
        assert res is not None
        collection_ids.extend(res.collection_ids())

    expected_collection_id = "reanalysis-era5-single-levels"
    assert expected_collection_id in collection_ids


def test_collections_limit(cat: catalogue.Catalogue) -> None:
    collections = cat.collections(params={"limit": 1})

    res = collections.next()

    if res is not None:
        assert res.response.status_code == 200


def test_collection(cat: catalogue.Catalogue) -> None:
    collection_id = "test-adaptor-mars"

    res = cat.collection(collection_id)

    assert isinstance(res, catalogue.Collection)
    assert "id" in res.json
    assert res.id == collection_id
    assert "links" in res.json
    assert isinstance(res.json["links"], list)
    assert res.begin_datetime.isoformat() == "1959-01-01T00:00:00+00:00"
    assert res.end_datetime.isoformat() == "2023-05-09T00:00:00+00:00"
