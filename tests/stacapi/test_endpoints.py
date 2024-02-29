import datetime as dt

import pytest
import pystac
import requests
from requests.auth import AuthBase
from pystac import Collection, Item, Extent, SpatialExtent, TemporalExtent
from yarl import URL


from stacbuilder.stacapi.endpoints import CollectionsEndpoint, RestApi


@pytest.fixture
def default_extent() -> Extent:
    return Extent(
        # Default spatial extent is the entire world.
        SpatialExtent([-180.0, -90.0, 180.0, 90.0]),
        # Default temporal extent is from 1 year ago up until now.
        TemporalExtent([[dt.datetime(2020, 1, 1), dt.datetime(2021, 1, 1)]]),
    )


@pytest.fixture
def test_provider() -> pystac.Provider:
    return pystac.Provider(
        name="ACME Faux GeoData Org", description="ACME providers of faux geodata", roles=[pystac.ProviderRole.PRODUCER]
    )


@pytest.fixture
def test_collection(test_provider, default_extent) -> Collection:
    return Collection(
        id="ACME-test-collection",
        title="Collection of faux ACME data",
        description="Collection of faux data from ACME org",
        keywords=["foo", "bar"],
        providers=[test_provider],
        extent=default_extent,
    )


@pytest.fixture
def test_items() -> list[Item]:
    return []


@pytest.fixture
def test_collection_with_items(test_collection, test_items) -> Collection:
    test_collection.add_items(test_items)
    test_collection.update_extent_from_items()
    return test_collection


class FauxAuth(AuthBase):
    def __call__(self, r):
        r.headers["Authorization"] = "magic-token"
        return r


class MockRestApi(RestApi):
    def __init__(self, base_url: URL | str, auth: AuthBase) -> None:
        super().__init__(base_url, auth)
        self.collections = {}
        self.items = {}

    def add_collection(self, collection: Collection) -> None:
        self.collections[collection.id] = collection

    def add_item(self, collection_id: str, item: Item) -> None:
        if collection_id not in self.collections:
            raise Exception("You have to add the collection with id={collection_id} before the item can be added")
        self.items[(collection_id, item.id)] = item

    def create(collection: Collection, items: list[Item]):
        api = MockRestApi()
        api.add_collection(collection)
        for item in items:
            api.add_item(collection.id, item)

        return api

    def get(self, url_path: str, *args, **kwargs) -> requests.Response:
        # return super().get(url_path, *args, **kwargs)
        path_parts = url_path.split("/")
        coll_id = path_parts[-1]
        if coll_id not in self.collections:
            # How to return HTTP 404 here?
            return requests.Response("???", status_code=404)


class TestRestApi:
    BASE_URL = URL("http://test.local/api")

    @pytest.fixture
    def api(self):
        return RestApi(self.BASE_URL, auth=None)

    def test_get(self, requests_mock, api):
        m = requests_mock.get(str(self.BASE_URL / "foo/bar"), status_code=200)
        api.get("foo/bar")
        assert m.called

    def test_post(self, requests_mock, api):
        m = requests_mock.post(str(self.BASE_URL / "foo/bar"), json=[1, 2, 3], status_code=200)
        api.post("foo/bar")
        assert m.called

    def test_put(self, requests_mock, api):
        m = requests_mock.put(str(self.BASE_URL / "foo/bar"), json=[1, 2, 3], status_code=200)
        api.put("foo/bar")
        assert m.called

    def test_delete(self, requests_mock, api):
        m = requests_mock.delete(str(self.BASE_URL / "foo/bar"), json=[1, 2, 3], status_code=200)
        api.delete("foo/bar")
        assert m.called


class TestCollectionsEndPoint:
    BASE_URL_STR = "https://test.stacapi.local"
    BASE_URL = URL(BASE_URL_STR)

    @pytest.fixture
    def collection_endpt(self) -> CollectionsEndpoint:
        return CollectionsEndpoint.create_endpoint(self.BASE_URL_STR, auth=None)

    def test_get(self, requests_mock, test_collection: Collection, collection_endpt: CollectionsEndpoint):
        m = requests_mock.get(
            str(self.BASE_URL / "collections" / test_collection.id), json=test_collection.to_dict(), status_code=200
        )
        actual_collection = collection_endpt.get(test_collection.id)
        assert test_collection.to_dict() == actual_collection.to_dict()
        assert m.called

    @pytest.mark.xfail(reason="Test not implemented yet")
    def test_get_all(self, requests_mock, test_collection: Collection, collection_endpt: CollectionsEndpoint):
        assert False, "Test not implemented yet"

    def test_create(self, requests_mock, test_collection: Collection, collection_endpt: CollectionsEndpoint):
        m = requests_mock.post(str(self.BASE_URL / "collections"), json=test_collection.to_dict(), status_code=201)
        response_json = collection_endpt.create(test_collection)
        assert test_collection.to_dict() == response_json
        assert m.called

    def test_update(self, requests_mock, test_collection: Collection, collection_endpt: CollectionsEndpoint):
        m = requests_mock.put(
            str(self.BASE_URL / "collections" / test_collection.id), json=test_collection.to_dict(), status_code=200
        )
        response_json = collection_endpt.update(test_collection)
        assert test_collection.to_dict() == response_json
        assert m.called

    def test_delete_by_id(self, requests_mock, test_collection: Collection, collection_endpt: CollectionsEndpoint):
        m = requests_mock.delete(
            str(self.BASE_URL / "collections" / test_collection.id), json=test_collection.to_dict(), status_code=200
        )
        collection_endpt.delete_by_id(test_collection.id)
        assert m.called

    def test_delete(self, requests_mock, test_collection: Collection, collection_endpt: CollectionsEndpoint):
        m = requests_mock.delete(
            str(self.BASE_URL / "collections" / test_collection.id), json=test_collection.to_dict(), status_code=200
        )
        collection_endpt.delete(test_collection)
        assert m.called


class TestItemsEndPoint:
    pass
