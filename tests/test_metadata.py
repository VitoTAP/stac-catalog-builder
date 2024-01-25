import datetime as dt
from dateutil.tz import tzoffset

import pytest
from shapely.geometry import Polygon

from stacbuilder.metadata import AssetMetadata
from stacbuilder.boundingbox import BoundingBox


class TestAssetMetadata:
    def test_constructor_sets_defaults(self):
        meta = AssetMetadata(extract_href_info=None, read_href_modifier=None)

        meta.asset_id is None
        meta.item_id is None
        meta.asset_type is None

        meta.href is None
        meta.original_href is None

        meta.shape is None
        meta.tags == []

        meta.transform is None
        meta.bbox_lat_lon is None
        meta.bbox_projected is None

        meta.bbox_as_list is None
        meta.proj_bbox_as_list is None
        meta.proj_epsg is None

        meta.geometry_as_dict is None
        meta.proj_geometry_as_dict is None
        meta.proj_geometry_as_wkt is None

        meta.version == "1.0.0"

        # TODO: band will be removed
        meta.band is None

        meta.datetime is None
        meta.start_datetime is None
        meta.end_datetime is None

        meta.year is None
        meta.month is None
        meta.day is None

        expected_dict = {
            "asset_id": None,
            "asset_path": None,
            "asset_type": None,
            "band": None,
            "bbox": None,
            "datetime": None,
            "day": None,
            "end_datetime": None,
            "geometry": None,
            "href": None,
            "item_id": None,
            "original_href": None,
            "month": None,
            "proj_bbox": None,
            "proj_epsg": None,
            "proj_geometry": None,
            "proj_geometry_as_wkt": None,
            "raster_metadata": None,
            "shape": None,
            "start_datetime": None,
            "tags": [],
            "year": None,
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
        meta = AssetMetadata(None, None)
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
        meta = AssetMetadata(None, None)
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
        meta = AssetMetadata(None, None)
        meta.end_datetime = in_value
        assert meta.end_datetime == expected

    @pytest.fixture
    def boundingboxes(self) -> BoundingBox:
        belgian_lambert = BoundingBox(624651.02, 687947.46, 694307.66, 799081.79, 3812)
        wgs84 = BoundingBox(4.0, 51.0576293, 4.9100677, 52.0, 3812)
        return belgian_lambert, wgs84

    @pytest.fixture
    def metadata_with_proj_bbox(self, boundingboxes) -> AssetMetadata:
        meta = AssetMetadata(None, None)
        meta.bbox_projected = boundingboxes[0]
        meta.bbox_lat_lon = boundingboxes[1]
        return meta

    def test_bbox_lat_lon(self):
        meta = AssetMetadata(None, None)
        bbox_wgs84 = BoundingBox(4.0, 51.0576293, 4.9100677, 52.0, 3812)
        meta.bbox_lat_lon = bbox_wgs84

        assert meta.bbox_lat_lon == bbox_wgs84
        assert meta.bbox_as_list == [4.0, 51.0576293, 4.9100677, 52.0]

    def test_bbox_projected(self):
        meta = AssetMetadata(None, None)
        bbox_proj = BoundingBox(624651.02, 687947.46, 694307.66, 799081.79, 3812)
        meta.bbox_projected = bbox_proj

        assert meta.bbox_projected == bbox_proj
        assert meta.proj_bbox_as_list == [624651.02, 687947.46, 694307.66, 799081.79]
        assert meta.proj_epsg == 3812

    def test_geometry_dict(self):
        meta = AssetMetadata(None, None)
        meta.bbox_lat_lon = BoundingBox(4.0, 51.0, 5.0, 52.0, 4326)
        expected = {
            "type": "Polygon",
            "coordinates": (
                # a single outer ring
                (
                    (4.0, 51.0),  # south-west / LL / min_x, min_y
                    (4.0, 52.0),  # north-west / UL / min_x, max_y
                    (5.0, 52.0),  # north-east / UR / max_x, max_y
                    (5.0, 51.0),  # south-east / LR / max_x, min_y
                    (4.0, 51.0),  # close the ring: last point = first point
                ),
            ),
        }
        assert meta.geometry_as_dict == expected

    def test_proj_epsg(self):
        meta = AssetMetadata(None, None)
        meta.bbox_projected = BoundingBox(624651.02, 687947.46, 694307.66, 799081.79, 3812)
        assert meta.proj_epsg == 3812

    def proj_geometry_as_dict(self):
        meta = AssetMetadata(None, None)
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
        assert meta.proj_geometry_as_dict == expected

    def test_geometry_as_wkt(self):
        meta = AssetMetadata(None, None)
        min_x = 624651.02
        min_y = 687947.46
        max_x = 694307.66
        max_y = 799081.79
        meta.bbox_projected = BoundingBox(min_x, min_y, max_x, max_y, 3812)
        expected_wkt = (
            f"POLYGON (({min_x} {min_y}, {min_x} {max_y}, {max_x} {max_y}, {max_x} {min_y}, {min_x} {min_y}))"
        )
        assert meta.proj_geometry_as_wkt == expected_wkt

    def test_proj_bbox_as_polygon(self):
        meta = AssetMetadata(None, None)
        min_x = 624651.02
        min_y = 687947.46
        max_x = 694307.66
        max_y = 799081.79
        meta.bbox_projected = BoundingBox(min_x, min_y, max_x, max_y, 3812)

        expected_polygon = Polygon.from_bounds(min_x, min_y, max_x, max_y)
        assert meta.proj_bbox_as_polygon == expected_polygon

    @pytest.mark.skip(reason="Test not yet implemented")
    @pytest.mark.xfail("Test not yet implemented")
    def test_process_href_info(self):
        # Important to cover this, so adding this it already as a nagging reminder.
        assert False, "Test not yet implemented"
