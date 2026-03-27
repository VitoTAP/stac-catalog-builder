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
from stacbuilder.stacapi.upload import Uploader, _merge_extents

API_BASE_URL = URL("http://test.stacapi.local")
API_BASE_URL_STR = str(API_BASE_URL)


def _make_extent(bbox, start, end) -> Extent:
    return Extent(
        SpatialExtent([bbox]),
        TemporalExtent([[start, end]]),
    )


def _utc(year, month, day) -> dt.datetime:
    """Return a UTC-aware datetime for use in tests."""
    return dt.datetime(year, month, day, tzinfo=dt.timezone.utc)


@pytest.fixture
def default_extent() -> Extent:
    return _make_extent([-180.0, -90.0, 180.0, 90.0], _utc(2020, 1, 1), _utc(2021, 1, 1))


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


class TestMergeExtents:
    """Unit tests for the _merge_extents helper function."""

    def test_spatial_extent_expanded(self):
        """The target bbox should be expanded to cover both collections' bboxes."""
        target = Collection(
            id="t",
            description="",
            extent=_make_extent([-10.0, -10.0, 10.0, 10.0], _utc(2022, 1, 1), _utc(2022, 12, 31)),
        )
        source = Collection(
            id="s",
            description="",
            extent=_make_extent([-20.0, -5.0, 30.0, 5.0], _utc(2021, 6, 1), _utc(2023, 6, 30)),
        )
        _merge_extents(target, source)

        merged_bbox = target.extent.spatial.bboxes[0]
        assert merged_bbox == [-20.0, -10.0, 30.0, 10.0]

    def test_temporal_extent_expanded(self):
        """The target temporal extent should be expanded to cover both intervals."""
        target = Collection(
            id="t",
            description="",
            extent=_make_extent([-180, -90, 180, 90], _utc(2022, 1, 1), _utc(2022, 12, 31)),
        )
        source = Collection(
            id="s",
            description="",
            extent=_make_extent([-180, -90, 180, 90], _utc(2021, 6, 1), _utc(2023, 6, 30)),
        )
        _merge_extents(target, source)

        start, end = target.extent.temporal.intervals[0]
        assert start == _utc(2021, 6, 1)
        assert end == _utc(2023, 6, 30)

    def test_none_temporal_bounds_handled(self):
        """None temporal bounds (open intervals) are handled without error.
        When either side has an unbounded boundary (None), the merged result is also unbounded (None)."""
        target = Collection(
            id="t",
            description="",
            extent=_make_extent([-180, -90, 180, 90], _utc(2022, 1, 1), None),
        )
        source = Collection(
            id="s",
            description="",
            extent=_make_extent([-180, -90, 180, 90], None, _utc(2023, 6, 30)),
        )
        _merge_extents(target, source)

        start, end = target.extent.temporal.intervals[0]
        # Both sides have a None boundary → merged is also None (open interval)
        assert start is None
        assert end is None


class TestMergeAndUploadCollection:
    """Tests for Uploader.merge_and_upload_collection()."""

    def test_creates_collection_when_not_exists(
        self, requests_mock, uploader: Uploader, empty_collection: Collection
    ):
        """When the collection does not exist on the API, it should be created (POST)."""
        coll_id = empty_collection.id

        requests_mock.get(str(API_BASE_URL / "collections" / coll_id), status_code=404)
        post_mock = requests_mock.post(
            str(API_BASE_URL / "collections"),
            json=empty_collection.to_dict(),
            status_code=201,
        )

        result = uploader.merge_and_upload_collection(empty_collection)

        assert result.id == coll_id
        assert post_mock.called

    def test_merges_extents_and_updates_when_exists(
        self, requests_mock, uploader: Uploader, empty_collection: Collection
    ):
        """When the collection exists, extents are merged and the collection is updated (PUT)."""
        coll_id = empty_collection.id

        # Existing collection on API has a smaller extent
        existing_extent = _make_extent(
            [-10.0, -10.0, 10.0, 10.0], _utc(2019, 1, 1), _utc(2019, 12, 31)
        )
        existing = Collection(
            id=coll_id,
            title="old title",
            description="old description",
            extent=existing_extent,
        )

        requests_mock.get(str(API_BASE_URL / "collections" / coll_id), json=existing.to_dict(), status_code=200)
        put_mock = requests_mock.put(
            str(API_BASE_URL / "collections" / coll_id),
            json=empty_collection.to_dict(),
            status_code=200,
        )
        post_mock = requests_mock.post(str(API_BASE_URL / "collections"), status_code=201)

        result = uploader.merge_and_upload_collection(empty_collection)

        assert result.id == coll_id
        # PUT (update) was called, POST (create) was not
        assert put_mock.called
        assert not post_mock.called

        # The new_collection's extents should have been expanded
        merged_bbox = result.extent.spatial.bboxes[0]
        assert merged_bbox[0] <= -10.0  # expanded west
        assert merged_bbox[1] <= -10.0  # expanded south
        assert merged_bbox[2] >= 10.0  # expanded east
        assert merged_bbox[3] >= 10.0  # expanded north

        merged_start, _ = result.extent.temporal.intervals[0]
        assert merged_start <= _utc(2019, 1, 1)

    def test_raises_type_error_for_invalid_input(self, uploader: Uploader):
        """A TypeError should be raised when the argument is not a Path or Collection."""
        with pytest.raises(TypeError):
            uploader.merge_and_upload_collection("not-a-collection")  # type: ignore[arg-type]

    def test_accepts_path_input(
        self, requests_mock, uploader: Uploader, empty_collection: Collection, tmp_path: Path
    ):
        """A Path pointing to a collection.json file should be accepted."""
        coll_id = empty_collection.id
        empty_collection.normalize_hrefs(str(tmp_path))
        empty_collection.save(dest_href=str(tmp_path))
        coll_path = tmp_path / "collection.json"

        requests_mock.get(str(API_BASE_URL / "collections" / coll_id), status_code=404)
        requests_mock.post(
            str(API_BASE_URL / "collections"),
            json=empty_collection.to_dict(),
            status_code=201,
        )

        result = uploader.merge_and_upload_collection(coll_path)

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

