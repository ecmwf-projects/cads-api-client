
from cads_api_client import catalogue, APIClient


def test_collections(api_root_url: str) -> None:

    client = APIClient(base_url=api_root_url)
    collections = client.collections()
    first_collection = next(collections)
    assert isinstance(first_collection, catalogue.Collection)
    # assert "collections" in res.json
    # assert isinstance(res.json["collections"], list)
    # assert "links" in res.json
    # assert isinstance(res.json["links"], list)
    expected_collection_id = "reanalysis-era5-single-levels"
    assert first_collection.id == expected_collection_id


def test_collections_limit(api_root_url: str) -> None:  # TODO
    client = APIClient(base_url=api_root_url)
    collections = client.collections(params={"limit": 1})
    collections_first_page = list(collections)
    expected_page_len = 56
    assert len(collections_first_page) == expected_page_len
    # res = collections.next()
    # if res is not None:
    #     assert res.response.status_code == 200


def test_collection(api_root_url: str) -> None:
    collection_id = "test-adaptor-mars"
    collection = catalogue.Collection(collection_id)
    assert isinstance(collection, catalogue.Collection)
    assert "id" in collection.json()
    assert collection.json()["id"] == collection_id
    assert "links" in collection.json()
    assert isinstance(collection.json()["links"], list)
