import datetime as dt
import re
from pathlib import Path

import pytest
from dateutil.tz import tzoffset
from shapely.geometry import Polygon, box

from stacbuilder.boundingbox import BoundingBox
from stacbuilder.metadata import AssetMetadata, check_datetime, convert_date_to_datetime


class TestAssetMetadata:
    @pytest.fixture
    def asset_metadata(self) -> AssetMetadata:
        return AssetMetadata(
            asset_id="test_asset_id",
            datetime=dt.datetime(2023, 10, 1, 12, 0, 0, tzinfo=dt.UTC),
            href="/local/path/to/asset.tif",
            original_href="/local/path/to/asset.tif",
            bbox_projected=BoundingBox(4.0, 51.0, 5.0, 52.0, 4326),
        )

    def test_constructor_sets_defaults(self, asset_metadata: AssetMetadata):
        assert asset_metadata.asset_id == "test_asset_id"
        assert asset_metadata.item_id == "test_asset_id"
        assert asset_metadata.asset_type is None
        assert asset_metadata.media_type is None

        assert asset_metadata.href == "/local/path/to/asset.tif"
        assert asset_metadata.original_href == "/local/path/to/asset.tif"

        assert asset_metadata.shape is None
        assert asset_metadata.tags == {}

        assert asset_metadata.transform is None
        assert asset_metadata.bbox_lat_lon == BoundingBox(4.0, 51.0, 5.0, 52.0, 4326)
        assert asset_metadata.bbox_projected == BoundingBox(4.0, 51.0, 5.0, 52.0, 4326)

        assert asset_metadata.bbox_as_list == [4.0, 51.0, 5.0, 52.0]
        assert asset_metadata.proj_bbox_as_list == [4.0, 51.0, 5.0, 52.0]
        assert asset_metadata.proj_epsg == 4326

        assert asset_metadata.proj_geometry_as_wkt is not None

        assert asset_metadata.datetime == dt.datetime(2023, 10, 1, 12, 0, 0, tzinfo=dt.UTC)
        assert asset_metadata.start_datetime is None
        assert asset_metadata.end_datetime is None

        assert asset_metadata.year == 2023
        assert asset_metadata.month == 10
        assert asset_metadata.day == 1

        assert asset_metadata.file_size is None

        assert asset_metadata.bands == []

        expected_dict = {
            "asset_id": "test_asset_id",
            "item_id": "test_asset_id",
            "tile_id": None,
            "href": "/local/path/to/asset.tif",
            "original_href": "/local/path/to/asset.tif",
            "asset_path": Path("/local/path/to/asset.tif"),
            "asset_type": None,
            "media_type": None,
            "datetime": dt.datetime(2023, 10, 1, 12, 0, 0, tzinfo=dt.UTC),
            "start_datetime": None,
            "end_datetime": None,
            "shape": None,
            "tags": {},
            "bbox_lat_lon": {"north": 52.0, "south": 51.0, "east": 5.0, "west": 4.0, "epsg": 4326},
            "bbox_projected": {"north": 52.0, "south": 51.0, "east": 5.0, "west": 4.0, "epsg": 4326},
            "geometry_lat_lon": Polygon(((5.0, 51.0), (5.0, 52.0), (4.0, 52.0), (4.0, 51.0), (5.0, 51.0))),
            "transform": None,
            "bands": [],
            "file_size": None,
        }

        assert expected_dict == asset_metadata.to_dict()

    def test_convert_date_to_datetime(self):
        in_date = dt.date(2024, 1, 21)
        expected_dt = dt.datetime(2024, 1, 21, 0, 0, 0, tzinfo=dt.UTC)
        actual_dt = convert_date_to_datetime(in_date)
        assert actual_dt == expected_dt

    @pytest.mark.parametrize(
        ["in_value", "expected"],
        [
            (dt.datetime(2024, 1, 20, 1, 2, 3, tzinfo=dt.UTC), dt.datetime(2024, 1, 20, 1, 2, 3, tzinfo=dt.UTC)),
            (dt.date(2024, 1, 21), dt.datetime(2024, 1, 21, 0, 0, 0, tzinfo=dt.UTC)),
            ("2023-09-10T12:13:14Z", dt.datetime(2023, 9, 10, 12, 13, 14, tzinfo=dt.UTC)),
            ("2023-09-10T12:13:14+00:00", dt.datetime(2023, 9, 10, 12, 13, 14, tzinfo=dt.UTC)),
            ("2023-09-10T12:13:14+01:00", dt.datetime(2023, 9, 10, 12, 13, 14, tzinfo=tzoffset(None, 3600))),
            ("2023-09-10T12:13:14-01:00", dt.datetime(2023, 9, 10, 12, 13, 14, tzinfo=tzoffset(None, -3600))),
        ],
    )
    def test_check_datetime(self, in_value, expected):
        actual_dt = check_datetime(in_value)
        assert actual_dt == expected

    @pytest.mark.parametrize(
        ["in_value", "expected"],
        [
            (dt.datetime(2024, 1, 20, 1, 2, 3, tzinfo=dt.UTC), dt.datetime(2024, 1, 20, 1, 2, 3, tzinfo=dt.UTC)),
            (dt.date(2024, 1, 21), dt.datetime(2024, 1, 21, 0, 0, 0, tzinfo=dt.UTC)),
            ("2023-09-10T12:13:14Z", dt.datetime(2023, 9, 10, 12, 13, 14, tzinfo=dt.UTC)),
            ("2023-09-10T12:13:14+00:00", dt.datetime(2023, 9, 10, 12, 13, 14, tzinfo=dt.UTC)),
            ("2023-09-10T12:13:14+01:00", dt.datetime(2023, 9, 10, 12, 13, 14, tzinfo=tzoffset(None, 3600))),
            ("2023-09-10T12:13:14-01:00", dt.datetime(2023, 9, 10, 12, 13, 14, tzinfo=tzoffset(None, -3600))),
        ],
    )
    def test_set_datetime(self, asset_metadata, in_value, expected):
        asset_metadata.datetime = in_value
        assert asset_metadata.datetime == expected

    @pytest.mark.parametrize(
        ["in_value", "expected"],
        [
            (dt.datetime(2024, 1, 20, 1, 2, 3, tzinfo=dt.UTC), dt.datetime(2024, 1, 20, 1, 2, 3, tzinfo=dt.UTC)),
            (dt.date(2024, 1, 21), dt.datetime(2024, 1, 21, 0, 0, 0, tzinfo=dt.UTC)),
            ("2023-09-10T12:13:14Z", dt.datetime(2023, 9, 10, 12, 13, 14, tzinfo=dt.UTC)),
            ("2023-09-10T12:13:14+00:00", dt.datetime(2023, 9, 10, 12, 13, 14, tzinfo=dt.UTC)),
            ("2023-09-10T12:13:14+01:00", dt.datetime(2023, 9, 10, 12, 13, 14, tzinfo=tzoffset(None, 3600))),
            ("2023-09-10T12:13:14-01:00", dt.datetime(2023, 9, 10, 12, 13, 14, tzinfo=tzoffset(None, -3600))),
        ],
    )
    def test_set_start_datetime(self, asset_metadata, in_value, expected):
        asset_metadata.start_datetime = in_value
        assert asset_metadata.start_datetime == expected

    @pytest.mark.parametrize(
        ["in_value", "expected"],
        [
            (dt.datetime(2024, 1, 20, 1, 2, 3, tzinfo=dt.UTC), dt.datetime(2024, 1, 20, 1, 2, 3, tzinfo=dt.UTC)),
            (dt.date(2024, 1, 21), dt.datetime(2024, 1, 21, 0, 0, 0, tzinfo=dt.UTC)),
            ("2023-09-10T12:13:14Z", dt.datetime(2023, 9, 10, 12, 13, 14, tzinfo=dt.UTC)),
            ("2023-09-10T12:13:14+00:00", dt.datetime(2023, 9, 10, 12, 13, 14, tzinfo=dt.UTC)),
            ("2023-09-10T12:13:14+01:00", dt.datetime(2023, 9, 10, 12, 13, 14, tzinfo=tzoffset(None, 3600))),
            ("2023-09-10T12:13:14-01:00", dt.datetime(2023, 9, 10, 12, 13, 14, tzinfo=tzoffset(None, -3600))),
        ],
    )
    def test_set_end_datetime(self, asset_metadata, in_value, expected):
        asset_metadata.end_datetime = in_value
        assert asset_metadata.end_datetime == expected

    @pytest.fixture
    def geometries(self) -> BoundingBox:
        bbox_belgian_lambert = BoundingBox(
            west=624112.728540544, south=687814.3689113414, east=694307.6687148043, north=799212.0443107984, epsg=3812
        )
        bbox_wgs84 = BoundingBox(
            west=4.0,
            south=51.0,
            east=5.0,
            north=52.0,
            epsg=4326,
        )

        geom_belgian_lambert = bbox_belgian_lambert.as_polygon()
        geom_wgs84 = Polygon(
            (
                (4.999983638156868, 50.99880365354712),
                (5.0139771225395915, 51.99992473464193),
                (3.9921549461460386, 52.001146622317236),
                (4, 51.00000000000001),
                (4.999983638156868, 50.99880365354712),
            )
        )

        epsg_belgian_lambert = 3812
        return bbox_belgian_lambert, bbox_wgs84, geom_belgian_lambert, geom_wgs84, epsg_belgian_lambert

    def test_bbox_lat_lon(self, geometries):
        _, bbox_wgs84, _, _, epsg = geometries
        meta = AssetMetadata(
            asset_id="test_asset_id",
            datetime=dt.datetime(2023, 10, 1, 12, 0, 0, tzinfo=dt.UTC),
            href="/local/path/to/asset.tif",
            original_href="/local/path/to/asset.tif",
            bbox_lat_lon=bbox_wgs84,
            file_size=123456,
            proj_epsg=epsg,
        )

        # assert meta.bbox_lat_lon == approx(bbox_wgs84, abs = 0.01)
        assert all(
            [
                meta.bbox_lat_lon.west == bbox_wgs84.west,
                meta.bbox_lat_lon.south == bbox_wgs84.south,
                meta.bbox_lat_lon.east == bbox_wgs84.east,
                meta.bbox_lat_lon.north == bbox_wgs84.north,
                meta.bbox_lat_lon.epsg == bbox_wgs84.epsg,
            ]
        )

        assert all(
            [
                meta.bbox_projected,
                meta.proj_epsg == epsg,
                meta.geometry_lat_lon,
                meta.geometry_proj,
            ]
        )

    def test_bbox_projected(self):
        bbox_proj = BoundingBox(624651.02, 687947.46, 694307.66, 799081.79, 3812)
        meta = AssetMetadata(
            asset_id="test_asset_id",
            datetime=dt.datetime(2023, 10, 1, 12, 0, 0, tzinfo=dt.UTC),
            href="/local/path/to/asset.tif",
            original_href="/local/path/to/asset.tif",
            file_size=123456,
            bbox_projected=bbox_proj,
        )

        assert meta.bbox_projected == bbox_proj
        assert meta.proj_bbox_as_list == [624651.02, 687947.46, 694307.66, 799081.79]
        assert meta.proj_epsg == 3812

    def test_geometry_dict(self):
        meta = AssetMetadata(
            asset_id="test_asset_id",
            datetime=dt.datetime(2023, 10, 1, 12, 0, 0, tzinfo=dt.UTC),
            href="/local/path/to/asset.tif",
            original_href="/local/path/to/asset.tif",
            file_size=123456,
            bbox_projected=BoundingBox(4.0, 51.0, 5.0, 52.0, 4326),
        )
        expected = {
            "type": "Polygon",
            "coordinates": (
                # a single outer ring
                (((5.0, 51.0), (5.0, 52.0), (4.0, 52.0), (4.0, 51.0), (5.0, 51.0))),
            ),
        }
        assert meta.geometry_lat_lon_as_dict == expected

    def proj_geometry_as_dict(self):
        meta = AssetMetadata(
            asset_id="test_asset_id",
            datetime=dt.datetime(2023, 10, 1, 12, 0, 0, tzinfo=dt.UTC),
            href="/local/path/to/asset.tif",
            original_href="/local/path/to/asset.tif",
            file_size=123456,
            bbox_projected=BoundingBox(624651.02, 687947.46, 694307.66, 799081.79, 3812),
        )
        expected = {
            "type": "Polygon",
            "coordinates": (
                # a single outer ring
                (
                    (624651.02, 687947.46),  # south-west / LL / min_x, min_y
                    (624651.02, 799081.79),  # north-west / UL / min_x, max_y
                    (694307.66, 799081.79),  # north-east / UR / max_x, max_y
                    (694307.66, 687947.46),  # south-east / LR / max_x, min_y
                    (624651.02, 687947.46),  # close the ring: last point = first point
                ),
            ),
        }
        assert meta.geometry_proj_as_dict == expected

    def test_geometry_as_wkt(self):
        min_x = 624651.02
        min_y = 687947.46
        max_x = 694307.66
        max_y = 799081.79
        meta = AssetMetadata(
            asset_id="test_asset_id",
            datetime=dt.datetime(2023, 10, 1, 12, 0, 0, tzinfo=dt.UTC),
            href="/local/path/to/asset.tif",
            original_href="/local/path/to/asset.tif",
            file_size=123456,
            bbox_projected=BoundingBox(min_x, min_y, max_x, max_y, 3812),
        )
        # expected_wkt = (
        #     f"POLYGON (({min_x} {min_y}, {min_x} {max_y}, {max_x} {max_y}, {max_x} {min_y}, {min_x} {min_y}))"
        # )
        expected_wkt = (
            f"POLYGON (({max_x} {min_y}, {max_x} {max_y}, {min_x} {max_y}, {min_x} {min_y}, {max_x} {min_y}))"
        )
        actual_wkt = meta.proj_geometry_as_wkt

        # keep only one fractional digit:
        actual_wkt = re.sub(r"([1-9]\d*\.[\d]{1})\d*", r"\1", actual_wkt)
        expected_wkt = re.sub(r"([1-9]\d*\.[\d]{1})\d*", r"\1", actual_wkt)
        assert actual_wkt == expected_wkt

    def test_proj_bbox_as_polygon(self):
        min_x = 624651.02
        min_y = 687947.46
        max_x = 694307.66
        max_y = 799081.79
        meta = AssetMetadata(
            asset_id="test_asset_id",
            datetime=dt.datetime(2023, 10, 1, 12, 0, 0, tzinfo=dt.UTC),
            href="/local/path/to/asset.tif",
            original_href="/local/path/to/asset.tif",
            file_size=123456,
            bbox_projected=BoundingBox(min_x, min_y, max_x, max_y, 3812),
        )

        expected_polygon = box(min_x, min_y, max_x, max_y)
        assert meta.proj_bbox_as_polygon == expected_polygon
