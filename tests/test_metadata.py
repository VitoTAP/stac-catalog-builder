# TODO: test coverage for AssetMetadata.to_dict
# TODO: test coverage for AssetMetadata.from_dict
# TODO: test coverage for AssetMetadata.from_geoseries

import datetime as dt
from dateutil.tz import tzoffset
import re
import pytest
from shapely.geometry import Polygon, box

from stacbuilder.metadata import AssetMetadata
from stacbuilder.boundingbox import BoundingBox


class TestAssetMetadata:
    def test_constructor_sets_defaults(self):
        meta = AssetMetadata(extract_href_info=None)

        assert meta.asset_id is None
        assert meta.item_id is None
        assert meta.asset_type is None
        assert meta.media_type is None
        assert meta.title is None

        assert meta.href is None
        assert meta.original_href is None
        assert meta.item_href is None

        assert meta.shape is None
        assert meta.tags == []

        assert meta.transform is None
        assert meta.bbox_lat_lon is None
        assert meta.bbox_projected is None
        assert meta.transform is None

        assert meta.bbox_as_list is None
        assert meta.proj_bbox_as_list is None
        assert meta.proj_epsg is None

        assert meta.proj_geometry_as_wkt is None

        assert meta.version == "1.0.0"

        assert meta.datetime is None
        assert meta.start_datetime is None
        assert meta.end_datetime is None

        assert meta.year is None
        assert meta.month is None
        assert meta.day is None

        assert meta.file_size is None

        expected_dict = {
            "asset_id": None,
            "item_id": None,
            "collection_id": None,
            "tile_id": None,
            "title": None,
            "href": None,
            "original_href": None,
            "item_href": None,
            "asset_path": None,
            "asset_type": None,
            "media_type": None,
            "datetime": None,
            "start_datetime": None,
            "end_datetime": None,
            "shape": None,
            "tags": [],
            "bbox_lat_lon": None,
            "bbox_projected": None,
            "geometry_lat_lon": None,
            "transform": None,
            "raster_metadata": None,
            "file_size": None,
        }

        assert expected_dict == meta.to_dict()

    def test_convert_date_to_datetime(self):
        in_date = dt.date(2024, 1, 21)
        expected_dt = dt.datetime(2024, 1, 21, 0, 0, 0, tzinfo=dt.UTC)
        actual_dt = AssetMetadata.convert_date_to_datetime(in_date)
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
        actual_dt = AssetMetadata.check_datetime(in_value)
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
    def test_set_datetime(self, in_value, expected):
        meta = AssetMetadata()
        meta.datetime = in_value
        assert meta.datetime == expected

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
    def test_set_start_datetime(self, in_value, expected):
        meta = AssetMetadata()
        meta.start_datetime = in_value
        assert meta.start_datetime == expected

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
    def test_set_end_datetime(self, in_value, expected):
        meta = AssetMetadata()
        meta.end_datetime = in_value
        assert meta.end_datetime == expected

    @pytest.fixture
    def boundingboxes(self) -> BoundingBox:
        belgian_lambert = BoundingBox(624651.02, 687947.46, 694307.66, 799081.79, 3812)
        wgs84 = BoundingBox(4.0, 51.0576293, 4.9100677, 52.0, 3812)
        return belgian_lambert, wgs84

    @pytest.fixture
    def metadata_with_proj_bbox(self, boundingboxes) -> AssetMetadata:
        meta = AssetMetadata()
        meta.bbox_projected = boundingboxes[0]
        meta.bbox_lat_lon = boundingboxes[1]
        return meta

    def test_bbox_lat_lon(self):
        meta = AssetMetadata()
        bbox_wgs84 = BoundingBox(4.0, 51.0576293, 4.9100677, 52.0, 3812)
        meta.bbox_lat_lon = bbox_wgs84

        assert meta.bbox_lat_lon == bbox_wgs84
        assert meta.bbox_as_list == [4.0, 51.0576293, 4.9100677, 52.0]

    def test_bbox_projected(self):
        meta = AssetMetadata()
        bbox_proj = BoundingBox(624651.02, 687947.46, 694307.66, 799081.79, 3812)
        meta.bbox_projected = bbox_proj

        assert meta.bbox_projected == bbox_proj
        assert meta.proj_bbox_as_list == [624651.02, 687947.46, 694307.66, 799081.79]
        assert meta.proj_epsg == 3812

    def test_geometry_dict(self):
        meta = AssetMetadata()
        meta.geometry_lat_lon = BoundingBox(4.0, 51.0, 5.0, 52.0, 4326).as_polygon()
        expected = {
            "type": "Polygon",
            "coordinates": (
                # a single outer ring
                (
                    ((5.0, 51.0), 
                     (5.0, 52.0),
                     (4.0, 52.0),
                     (4.0, 51.0),
                     (5.0, 51.0))
                ),
            ),
        }
        assert meta.geometry_lat_lon_as_dict == expected

    def test_proj_epsg(self):
        meta = AssetMetadata()
        meta.bbox_projected = BoundingBox(624651.02, 687947.46, 694307.66, 799081.79, 3812)
        assert meta.proj_epsg == 3812

    def proj_geometry_as_dict(self):
        meta = AssetMetadata()
        meta.bbox_projected = BoundingBox(624651.02, 687947.46, 694307.66, 799081.79, 3812)
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
        meta = AssetMetadata()
        min_x = 624651.02
        min_y = 687947.46
        max_x = 694307.66
        max_y = 799081.79
        meta.bbox_projected = BoundingBox(min_x, min_y, max_x, max_y, 3812)
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
        meta = AssetMetadata()
        min_x = 624651.02
        min_y = 687947.46
        max_x = 694307.66
        max_y = 799081.79
        meta.bbox_projected = BoundingBox(min_x, min_y, max_x, max_y, 3812)

        expected_polygon = box(min_x, min_y, max_x, max_y)
        assert meta.proj_bbox_as_polygon == expected_polygon

    @pytest.mark.skip(reason="Test not yet implemented")
    @pytest.mark.xfail("Test not yet implemented")
    def test_process_href_info(self):
        # Important to cover this, so adding this it already as a nagging reminder.
        assert False, "Test not yet implemented"
