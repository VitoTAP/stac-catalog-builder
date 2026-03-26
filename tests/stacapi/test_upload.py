"""Tests for stacbuilder.stacapi.upload module."""
import datetime as dt
import json
from pathlib import Path
from typing import List

import pystac
import pytest
import shapely
from pystac import (
    Asset,
    Collection,
    Extent,
    Item,
    SpatialExtent,
    TemporalExtent,
)
from yarl import URL

from stacbuilder.boundingbox import BoundingBox
from stacbuilder.stacapi.endpoints import CollectionsEndpoint, ItemsEndpoint, RestApi
from stacbuilder.stacapi.upload import Uploader

API_BASE_URL = URL("http://test.stacapi.local")
API_BASE_URL_STR = str(API_BASE_URL)


@pytest.fixture
def default_extent() -> Extent:
    return Extent(
        SpatialExtent([-180.0, -90.0, 180.0, 90.0]),
        TemporalExtent([[dt.datetime(2020, 1, 1), dt.datetime(2021, 1, 1)]]),
    )


@pytest.fixture
def provider() -> pystac.Provider:
    return pystac.Provider(
        name="ACME Faux GeoData Org",
        description="ACME providers of faux geodata",
        roles=[pystac.ProviderRole.PRODUCER],
    )


@pytest.fixture
def empty_collection(provider, default_extent) -> Collection:
    collection = Collection(
        id="ACME-test-collection",
        title="Collection of faux ACME data",
        description="Collection of faux data from ACME org",
        keywords=["foo", "bar"],
        providers=[provider],
        extent=default_extent,
    )
    return collection


def _create_item(item_id: str, collection_id: str, base_dir: Path) -> Item:
    bbox_list = [-180, -90, 180, 90]
    geometry = BoundingBox.from_list(bbox_list, epsg=4326).as_geometry_dict()
    polygon = shapely.from_geojson(json.dumps(geometry))
    geo_dict = json.loads(shapely.to_geojson(polygon))

    # The path is used as an href string only; the file is never accessed.
    asset_path = base_dir / f"{item_id}.tif"
    asset = Asset(
        href=str(asset_path),
        title=item_id,
        media_type=pystac.MediaType.COG,
        roles=["data"],
    )

    item = pystac.Item(
        id=item_id,
        assets={"data": asset},
        bbox=bbox_list,
        geometry=geo_dict,
        datetime=dt.datetime(2024, 1, 1),
        properties={},
        href=f"./{item_id}.json",
    )
    # Set collection relationship via a link so the item passes STAC validation
    item.add_link(pystac.Link(rel=pystac.RelType.COLLECTION, target=collection_id))
    item.collection_id = collection_id
    return item


@pytest.fixture
def uploader() -> Uploader:
    return Uploader.create_uploader(stac_api_url=API_BASE_URL_STR, auth=None, bulk_size=2)


class TestGetOrCreateCollection:
    """Tests for Uploader.get_or_create_collection()."""

    def test_creates_collection_when_not_exists(
        self, requests_mock, uploader: Uploader, empty_collection: Collection
    ):
        """When the collection does not exist on the API, it should be created."""
        coll_id = empty_collection.id

        # First call (exists check) returns 404
        requests_mock.get(str(API_BASE_URL / "collections" / coll_id), status_code=404)
        # Second call (create) returns 201
        requests_mock.post(
            str(API_BASE_URL / "collections"),
            json=empty_collection.to_dict(),
            status_code=201,
        )

        result = uploader.get_or_create_collection(empty_collection)

        assert result.id == coll_id

    def test_returns_existing_collection_when_already_exists(
        self, requests_mock, uploader: Uploader, empty_collection: Collection
    ):
        """When the collection already exists, the existing collection should be returned
        and no create (POST) call should be made."""
        coll_id = empty_collection.id

        # The existence check (GET) returns 200, and also serves the full collection dict
        get_mock = requests_mock.get(
            str(API_BASE_URL / "collections" / coll_id),
            json=empty_collection.to_dict(),
            status_code=200,
        )
        # POST should never be called
        post_mock = requests_mock.post(str(API_BASE_URL / "collections"), status_code=201)

        result = uploader.get_or_create_collection(empty_collection)

        assert result.id == coll_id
        # GET was called (at least the existence check + the fetch)
        assert get_mock.called
        # POST (create) must not have been called
        assert not post_mock.called

    def test_raises_type_error_for_invalid_input(self, uploader: Uploader):
        """A TypeError should be raised when something other than Path or Collection is passed."""
        with pytest.raises(TypeError):
            uploader.get_or_create_collection("not-a-collection")  # type: ignore[arg-type]

    def test_accepts_path_input(
        self, requests_mock, uploader: Uploader, empty_collection: Collection, tmp_path: Path
    ):
        """A Path pointing to a collection.json file should be accepted."""
        coll_id = empty_collection.id
        coll_path = tmp_path / "collection.json"
        empty_collection.normalize_hrefs(str(tmp_path))
        empty_collection.save(dest_href=str(tmp_path))

        # Existence check returns 404 → will create
        requests_mock.get(str(API_BASE_URL / "collections" / coll_id), status_code=404)
        requests_mock.post(
            str(API_BASE_URL / "collections"),
            json=empty_collection.to_dict(),
            status_code=201,
        )

        result = uploader.get_or_create_collection(coll_path)

        assert result.id == coll_id


class TestUploadItemsBulk:
    """Tests for Uploader.upload_items_bulk() to verify queue/bulk behaviour."""

    def test_bulk_upload_splits_into_chunks(
        self, requests_mock, uploader: Uploader, empty_collection: Collection, tmp_path: Path
    ):
        """Items should be uploaded in chunks of bulk_size."""
        coll_id = empty_collection.id
        items = [_create_item(f"item-{i:02d}", coll_id, tmp_path) for i in range(5)]

        bulk_mock = requests_mock.post(
            str(API_BASE_URL / "collections" / coll_id / "bulk_items"),
            json={"message": "ok"},
            status_code=200,
        )

        uploader.upload_items_bulk(coll_id, items)

        # With bulk_size=2 and 5 items we expect 3 POST calls (chunks of 2, 2, 1)
        assert bulk_mock.call_count == 3

    def test_bulk_upload_no_items(self, requests_mock, uploader: Uploader, empty_collection: Collection):
        """Uploading an empty list should succeed without making any API calls."""
        coll_id = empty_collection.id
        bulk_mock = requests_mock.post(
            str(API_BASE_URL / "collections" / coll_id / "bulk_items"),
            json={"message": "ok"},
            status_code=200,
        )

        uploader.upload_items_bulk(coll_id, [])

        assert bulk_mock.call_count == 0
