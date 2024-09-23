import re

import pytest

from shapely.geometry import Polygon, box


from stacbuilder.boundingbox import (
    bbox_dict_to_list,
    bbox_list_to_dict,
    to_bbox_dict,
    BoundingBox,
)


class TestSimpleConversionFunctions:
    def test_bbox_dict_to_list(self):
        input_bbox = {"west": 1.0, "south": 2.0, "east": 3.0, "north": 4.0}

        actual_list = bbox_dict_to_list(input_bbox)

        expected_list = [1.0, 2.0, 3.0, 4.0]
        assert actual_list == expected_list

    def test_bbox_list_to_dict(self):
        input_list = [1.0, 2.0, 3.0, 4.0]
        actual_bbox = bbox_list_to_dict(input_list)

        expected_bbox = {"west": 1.0, "south": 2.0, "east": 3.0, "north": 4.0}
        assert actual_bbox == expected_bbox

    def test_to_bbox_dict(self):
        expected_bbox = {"west": 1.0, "south": 2.0, "east": 3.0, "north": 4.0}
        actual_bbox = to_bbox_dict(1.0, 2.0, 3.0, 4.0)
        assert actual_bbox == expected_bbox


class TestBoundingBox:
    def test_create_empty(self):
        empty_bbox = BoundingBox.create_empty()

        assert empty_bbox.west == 0.0
        assert empty_bbox.min_x == 0.0

        assert empty_bbox.south == 0.0
        assert empty_bbox.min_y == 0.0

        assert empty_bbox.east == 0.0
        assert empty_bbox.max_x == 0.0

        assert empty_bbox.north == 0.0
        assert empty_bbox.max_y == 0.0

        assert empty_bbox.epsg is None

    @pytest.mark.parametrize(
        ["west", "south", "east", "north", "epsg"],
        [
            (1.0, 2.0, 3.0, 4.0, None),
            (1.0, 2.0, 3.0, 4.0, 4326),
            (-180.0, -90.0, 180.0, 90.0, 4326),
            (624651.02, 687947.46, 694307.66, 799081.79, 3812),
        ],
    )
    def test_constructor_positional_args(self, west, south, east, north, epsg):
        bbox = BoundingBox(west, south, east, north, epsg)

        assert bbox.west == west
        assert bbox.min_x == west

        assert bbox.south == south
        assert bbox.min_y == south

        assert bbox.east == east
        assert bbox.max_x == east

        assert bbox.north == north
        assert bbox.max_y == north

        assert bbox.epsg == epsg

    @pytest.mark.parametrize(
        ["west", "south", "east", "north", "epsg"],
        [
            (1.0, 2.0, 3.0, 4.0, None),
            (1.0, 2.0, 3.0, 4.0, 4326),
            (-180.0, -90.0, 180.0, 90.0, 4326),
            (624651.02, 687947.46, 694307.66, 799081.79, 3812),
            (624651.02, 687947.46, 694307.66, 799081.79, "EPSG:3812"),
        ],
    )
    def test_constructor_keyword_args(self, west, south, east, north, epsg):
        bbox = BoundingBox(west=west, south=south, east=east, north=north, epsg=epsg)

        assert bbox.west == west
        assert bbox.min_x == west

        assert bbox.south == south
        assert bbox.min_y == south

        assert bbox.east == east
        assert bbox.max_x == east

        assert bbox.north == north
        assert bbox.max_y == north

        assert bbox.epsg == epsg

    def test_to_dict(self):
        bbox = BoundingBox(10.0, 20.0, 30.0, 40.0, 3812)
        expected_dict = {"west": 10.0, "south": 20.0, "east": 30.0, "north": 40.0, "epsg": 3812}
        assert bbox.to_dict() == expected_dict

    def test_set_from_dict(self):
        bbox = BoundingBox.create_empty()
        input_dict = {"west": 10.0, "south": 20.0, "east": 30.0, "north": 40.0, "epsg": 3812}
        bbox.set_from_dict(input_dict)

        expected_bbox = BoundingBox(10.0, 20.0, 30.0, 40.0, 3812)
        assert bbox == expected_bbox

    def test_from_dict(self):
        input_dict = {"west": 10.0, "south": 20.0, "east": 30.0, "north": 40.0, "epsg": 3812}
        actual_bbox = BoundingBox.from_dict(input_dict)

        expected_bbox = BoundingBox(10.0, 20.0, 30.0, 40.0, 3812)
        assert actual_bbox == expected_bbox

    def test_to_list(self):
        bbox = BoundingBox(10.0, 20.0, 30.0, 40.0, 3812)
        expected_list = [10.0, 20.0, 30.0, 40.0]
        assert bbox.to_list() == expected_list

    def test_set_from_list(self):
        input_list = [10.0, 20.0, 30.0, 40.0]
        actual_bbox = BoundingBox.create_empty()
        actual_bbox.set_from_list(input_list, epsg=3812)

        expected_bbox = BoundingBox(10.0, 20.0, 30.0, 40.0, 3812)
        assert actual_bbox == expected_bbox

    def test_from_list(self):
        input_list = [10.0, 20.0, 30.0, 40.0]
        actual_bbox = BoundingBox.from_list(input_list, epsg=3812)

        expected_bbox = BoundingBox(10.0, 20.0, 30.0, 40.0, 3812)
        assert actual_bbox == expected_bbox

    def test_as_polygon(self):
        bbox = BoundingBox(10.0, 20.0, 30.0, 40.0, 3812)
        actual_polygon = bbox.as_polygon()

        # Note: Shapely basically ignores the CRS. We don't verify it here.
        # TODO:  If we do find a way to add the CRS, we should verify that too.
        # expected_polygon = Polygon.from_bounds(10.0, 20.0, 30.0, 40.0)
        expected_polygon = box(10.0, 20.0, 30.0, 40.0)
        assert actual_polygon == expected_polygon

    def test_as_wkt(self):
        bbox = BoundingBox(10.0, 20.0, 30.0, 40.0, 3812)
        # Note that standard Well-Known Text does not include the Coordinate Reference System.
        # EWKT or "Extended WKT" does include it, but this is not what we are working with here.
        # (EWKT is used by PostGIS for example)
        expected_wkt = "POLYGON ((30 20, 30 40, 10 40, 10 20, 30 20))"
        actual_wkt = bbox.as_wkt()
        # Truncate floating points to int:
        actual_wkt = re.sub(r"(\d+)\.\d{2}\d*", r"\1", actual_wkt)
        assert actual_wkt == expected_wkt

    def test_as_geometry_dict(self):
        bbox = BoundingBox(10.0, 20.0, 30.0, 40.0, 3812)
        # Note: Shapely basically ignores the CRS.
        expected = {
            "type": "Polygon",
            "coordinates": (
                (
                    (30.0, 20.0), # south-east / LR / max_x, min_y
                    (30.0, 40.0), # north-east / UR / max_x, max_y
                    (10.0, 40.0), # north-west / UL / min_x, max_y
                    (10.0, 20.0), # south-west / LL / min_x, min_y
                    (30.0, 20.0) # close the ring: last point = first point
                ),
            ),
        }
        assert bbox.as_geometry_dict() == expected
