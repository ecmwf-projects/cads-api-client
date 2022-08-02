from cads_api_client import catalogue


def test_collections(dev_env_api_url: str) -> None:
    cat = catalogue.Catalogue(f"{dev_env_api_url}/catalogue")

    res = cat.collections()

    assert isinstance(res, dict)
    assert "collections" in res
    assert isinstance(res["collections"], list)
    assert "links" in res
    assert isinstance(res["links"], list)


def test_collection_ids(dev_env_api_url: str) -> None:
    collection_id = "reanalysis-era5-land-monthly-means"
    cat = catalogue.Catalogue(f"{dev_env_api_url}/catalogue")

    res = cat.collection_ids()

    assert isinstance(res, list)
    assert collection_id in res


def test_collection(dev_env_api_url: str) -> None:
    collection_id = "reanalysis-era5-land-monthly-means"
    cat = catalogue.Catalogue(f"{dev_env_api_url}/catalogue")

    res = cat.collection(collection_id)

    assert isinstance(res, catalogue.Collection)
    assert "id" in res.json
    assert res.json["id"] == collection_id
    assert "links" in res.json
    assert isinstance(res.json["links"], list)
