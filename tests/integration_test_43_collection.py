import pytest

from cads_api_client import ApiClient, Collection, Remote, processing


@pytest.fixture
def collection(api_anon_client: ApiClient) -> Collection:
    return api_anon_client.get_collection("test-adaptor-mars")


def test_collection_begin_datetime(collection: Collection) -> None:
    assert collection.begin_datetime.isoformat() == "1959-01-01T00:00:00+00:00"


def test_collection_end_datetime(collection: Collection) -> None:
    assert collection.end_datetime.isoformat() == "2023-05-09T00:00:00+00:00"


def test_collection_id(collection: Collection) -> None:
    assert collection.id == "test-adaptor-mars"


def test_collection_process(collection: Collection) -> None:
    process = collection.process
    assert isinstance(process, processing.Process)
    assert process.id == "test-adaptor-mars"


def test_collection_submit(collection: Collection) -> None:
    assert isinstance(collection.submit(), Remote)
