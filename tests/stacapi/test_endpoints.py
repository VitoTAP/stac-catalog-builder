import datetime as dt
import json
from pathlib import Path

import pytest
import pystac
import shapely
from pystac import Asset, Collection, Item, ItemCollection, Extent, SpatialExtent, TemporalExtent
from yarl import URL


from stacbuilder.stacapi.endpoints import CollectionsEndpoint, ItemsEndpoint, RestApi
from stacbuilder.boundingbox import BoundingBox


API_BASE_URL = URL("http://test.stacapi.local")


@pytest.fixture
def default_extent() -> Extent:
    return Extent(
        # Default spatial extent is the entire world.
        SpatialExtent([-180.0, -90.0, 180.0, 90.0]),
        # Default temporal extent is from 1 year ago up until now.
        TemporalExtent([[dt.datetime(2020, 1, 1), dt.datetime(2021, 1, 1)]]),
    )


@pytest.fixture
def provider() -> pystac.Provider:
    return pystac.Provider(
        name="ACME Faux GeoData Org", description="ACME providers of faux geodata", roles=[pystac.ProviderRole.PRODUCER]
    )


@pytest.fixture
def empty_collection(provider, default_extent) -> Collection:
    coll_id = "ACME-test-collection"
    collection = Collection(
        id=coll_id,
        title="Collection of faux ACME data",
        description="Collection of faux data from ACME org",
        keywords=["foo", "bar"],
        providers=[provider],
        extent=default_extent,
    )
    return collection


def create_asset(asset_path: Path) -> Asset:
    return Asset(
        href=str(asset_path),
        title=asset_path.stem,
        description=f"GeoTIFF File {asset_path.stem}",
        media_type=pystac.MediaType.COG,
        roles=["data"],
    )


@pytest.fixture
def asset_paths(data_dir) -> dict[str, Path]:
    return {
        "t2m": data_dir / "2000/observations_2m-temp-monthly_2000-01-01.tif",
        "pr_tot": data_dir / "2000/observations_tot-precip-monthly_2000-01-01.tif",
    }


@pytest.fixture
def fake_assets(asset_paths: dict[str, Path]) -> dict[str, Asset]:
    return {asset_type: create_asset(path) for asset_type, path in asset_paths.items()}


def create_item(item_id: str, fake_assets) -> Item:
    bbox_list = [-180, -90, 180, 90]
    geometry = BoundingBox.from_list(bbox_list, epsg=4326).as_geometry_dict()

    polygon: shapely.Polygon = shapely.from_geojson(json.dumps(geometry))
    geo_json = shapely.to_geojson(polygon)
    geo_dict = json.loads(geo_json)

    item = pystac.Item(
        id=item_id,
        assets=fake_assets,
        bbox=bbox_list,
        geometry=geo_dict,
        datetime=dt.datetime(2024, 1, 1),
        properties={},
        href=f"./{item_id}.json",
    )

    item.validate()

    return item


@pytest.fixture
def single_item(fake_assets) -> Item:
    return create_item("items01", fake_assets)


@pytest.fixture
def multiple_items(fake_assets) -> Item:
    return [create_item("items01", fake_assets), create_item("items02", fake_assets)]


def feature_collection(multiple_items) -> ItemCollection:
    return ItemCollection(items=multiple_items)


@pytest.fixture
def collection_with_items(empty_collection, multiple_items) -> Collection:
    item: Item
    for item in multiple_items:
        item.collection = empty_collection

    empty_collection.add_items(multiple_items)
    empty_collection.update_extent_from_items()
    empty_collection.make_all_asset_hrefs_relative()

    return empty_collection


class TestRestApi:
    BASE_URL = API_BASE_URL

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
    BASE_URL = API_BASE_URL
    BASE_URL_STR = str(API_BASE_URL)

    @pytest.fixture
    def collection_endpt(self) -> CollectionsEndpoint:
        return CollectionsEndpoint.create_endpoint(self.BASE_URL_STR, auth=None)

    def test_get(self, requests_mock, empty_collection: Collection, collection_endpt: CollectionsEndpoint):
        m = requests_mock.get(
            str(self.BASE_URL / "collections" / empty_collection.id), json=empty_collection.to_dict(), status_code=200
        )
        actual_collection = collection_endpt.get(empty_collection.id)
        assert empty_collection.to_dict() == actual_collection.to_dict()
        assert m.called

    def test_create(self, requests_mock, empty_collection: Collection, collection_endpt: CollectionsEndpoint):
        m = requests_mock.post(str(self.BASE_URL / "collections"), json=empty_collection.to_dict(), status_code=201)
        response_json = collection_endpt.create(empty_collection)
        assert empty_collection.to_dict() == response_json
        assert m.called

    def test_update(self, requests_mock, empty_collection: Collection, collection_endpt: CollectionsEndpoint):
        m = requests_mock.put(
            str(self.BASE_URL / "collections" / empty_collection.id), json=empty_collection.to_dict(), status_code=200
        )
        response_json = collection_endpt.update(empty_collection)
        assert empty_collection.to_dict() == response_json
        assert m.called

    def test_delete_by_id(self, requests_mock, empty_collection: Collection, collection_endpt: CollectionsEndpoint):
        m = requests_mock.delete(
            str(self.BASE_URL / "collections" / empty_collection.id), json=empty_collection.to_dict(), status_code=200
        )
        collection_endpt.delete_by_id(empty_collection.id)
        assert m.called

    def test_delete(self, requests_mock, empty_collection: Collection, collection_endpt: CollectionsEndpoint):
        m = requests_mock.delete(
            str(self.BASE_URL / "collections" / empty_collection.id), json=empty_collection.to_dict(), status_code=200
        )
        collection_endpt.delete(empty_collection)
        assert m.called


class TestItemsEndPoint:
    BASE_URL = API_BASE_URL
    BASE_URL_STR = str(API_BASE_URL)

    @pytest.fixture(autouse=True)
    def items_endpt(self) -> ItemsEndpoint:
        return ItemsEndpoint.create_endpoint(self.BASE_URL_STR, auth=None)

    def test_get(self, requests_mock, collection_with_items: Collection, items_endpt: ItemsEndpoint):
        items = list(collection_with_items.get_all_items())
        expected_item: Item = items[0]
        m = requests_mock.get(
            str(self.BASE_URL / "collections" / collection_with_items.id / "items" / expected_item.id),
            json=expected_item.to_dict(),
            status_code=200,
        )
        actual_item: Item = items_endpt.get(collection_with_items.id, expected_item.id)
        assert expected_item.id == actual_item.id
        assert expected_item.collection_id == actual_item.collection_id
        assert expected_item.bbox == actual_item.bbox

        assert len(expected_item.assets) == len(actual_item.assets)
        for asset_type, expected_asset in expected_item.assets.items():
            assert asset_type in actual_item.assets
            assert expected_asset.to_dict() == actual_item.assets[asset_type].to_dict()

        assert m.called

    @pytest.mark.skip(reason="Test not yet correct, ItemCollection does not work yet")
    def test_get_all(self, requests_mock, collection_with_items: Collection, items_endpt: ItemsEndpoint):
        collection_path = Path(collection_with_items.self_href)
        if not collection_path.parent.exists():
            collection_path.mkdir(parents=True)
        collection_with_items.save(catalog_type=pystac.CatalogType.SELF_CONTAINED)

        expected_items = list(collection_with_items.get_all_items())
        expected_item_collection = ItemCollection(expected_items)
        data = expected_item_collection.to_dict()
        m = requests_mock.get(
            str(self.BASE_URL / "collections" / collection_with_items.id / "items"), json=data, status_code=200
        )
        actual_item_collection = items_endpt.get_all(collection_with_items.id)

        assert expected_item_collection == actual_item_collection
        assert m.called

    def test_create(self, requests_mock, collection_with_items: Collection, items_endpt: ItemsEndpoint, tmp_path):
        collection_dir = tmp_path / "STAC" / collection_with_items.id
        collection_with_items.set_self_href(str(collection_dir))

        items = list(collection_with_items.get_all_items())
        item = items[0]
        m = requests_mock.post(
            str(self.BASE_URL / "collections" / collection_with_items.id / "items"),
            json=item.to_dict(),
            status_code=201,
        )
        actual_dict: dict = items_endpt.create(item)
        assert item.to_dict() == actual_dict
        assert m.called

    def test_update(self, requests_mock, collection_with_items: Collection, items_endpt: ItemsEndpoint, tmp_path):
        collection_dir = tmp_path / "STAC" / collection_with_items.id
        collection_with_items.set_self_href(str(collection_dir))

        item = list(collection_with_items.get_all_items())[0]
        m = requests_mock.put(
            str(self.BASE_URL / "collections" / collection_with_items.id / "items" / item.id),
            json=item.to_dict(),
            status_code=200,
        )
        actual_dict: dict = items_endpt.update(item)
        assert item.to_dict() == actual_dict
        assert m.called
