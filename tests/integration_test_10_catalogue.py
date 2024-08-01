from cads_api_client import catalogue


def test_collections(api_root_url: str) -> None:
    cat = catalogue.Catalogue(f"{api_root_url}/catalogue")

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


def test_collections_limit(api_root_url: str) -> None:
    cat = catalogue.Catalogue(f"{api_root_url}/catalogue")
    collections = cat.collections(params={"limit": 1})

    res = collections.next()

    if res is not None:
        assert res.response.status_code == 200


def test_collection(api_root_url: str) -> None:
    collection_id = "test-adaptor-mars"
    cat = catalogue.Catalogue(f"{api_root_url}/catalogue")

    res = cat.collection(collection_id)

    assert isinstance(res, catalogue.Collection)
    assert "id" in res.json
    assert res.id == collection_id
    assert "links" in res.json
    assert isinstance(res.json["links"], list)
