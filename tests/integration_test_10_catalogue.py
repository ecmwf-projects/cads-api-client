from cads_api_client import catalogue


def test_collections() -> None:
    cat = catalogue.Catalogue("http://localhost:8080/api/catalogue")

    res = cat.collections()

    assert isinstance(res, dict)
    assert "collections" in res
    assert isinstance(res["collections"], list)
    assert "links" in res
    assert isinstance(res["links"], list)


def test_collection_ids() -> None:
    collection_id = "reanalysis-era5-land-monthly-means"
    cat = catalogue.Catalogue("http://localhost:8080/api/catalogue")

    res = cat.collection_ids()

    assert isinstance(res, list)
    assert collection_id in res


def test_collection() -> None:
    collection_id = "reanalysis-era5-land-monthly-means"
    cat = catalogue.Catalogue("http://localhost:8080/api/catalogue/")

    res = cat.collection(collection_id)

    assert isinstance(res, dict)
    assert "id" in res
    assert res["id"] == collection_id
    assert "links" in res
    assert isinstance(res["links"], list)
