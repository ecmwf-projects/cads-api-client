from cads_api_client import catalogue


def test_collections(dev_env_api_url: str) -> None:
    cat = catalogue.Catalogue(f"{dev_env_api_url}/catalogue")

    res = cat.collections()

    assert isinstance(res, catalogue.Collections)
    assert "collections" in res.json
    assert isinstance(res.json["collections"], list)
    assert "links" in res.json
    assert isinstance(res.json["links"], list)

    expected_collection_id = "reanalysis-era5-land-monthly-means"

    assert expected_collection_id in res.collection_ids()


def test_collection(dev_env_api_url: str) -> None:
    collection_id = "reanalysis-era5-land-monthly-means"
    cat = catalogue.Catalogue(f"{dev_env_api_url}/catalogue")

    res = cat.collection(collection_id)

    assert isinstance(res, catalogue.Collection)
    assert "id" in res.json
    assert res.json["id"] == collection_id
    assert "links" in res.json
    assert isinstance(res.json["links"], list)
