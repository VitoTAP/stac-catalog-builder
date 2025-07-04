import pytest
from pytest import approx
from shapely.geometry import box

from stacbuilder.boundingbox import bbox_dict_to_list
from stacbuilder.projections import (
    get_transform,
    project_polygon,
    reproject_bounding_box,
)

BBOX_TABLE = [
    (
        # Belgian Lambert 2008
        # This test case should have least problems since the area is really Belgium.
        3812,
        {
            "west": 624112.728540544,
            "south": 687814.3689113414,
            "east": 694307.6687148043,
            "north": 799212.0443107984,
        },
    ),
    (
        # ETRS89-extended / LAEA Europe
        # Covering this case is important.
        # With LAEA Europe you can come across problems to get a good coordinate transform.
        # Watch out if you get so-called "ball park" transforms. This happens in QGIS.
        3035,
        {
            "west": 3900350.772802173,
            "south": 3110735.7505430346,
            "east": 3977921.1759082996,
            "north": 3226952.0036674426,
        },
    ),
    (
        # ETRS89 / UTM zone 31N (N-E)"
        3043,
        {
            "west": 568649.7048958719,
            "south": 5650300.786521471,
            "east": 640333.2963397139,
            "north": 5762926.812790221,
        },
    ),
    (
        # Netherlands, Amersfoort / RD New
        # Even though the area is outside The Netherlands, the result should still be OK.
        28992,
        {
            "west": 57624.62876501742,
            "south": 334555.355807676,
            "east": 128410.08537081015,
            "north": 446645.1944649341,
        },
    ),
    (
        # Also test no-operation: CRSs are identical, both WGS84
        4326,
        {
            "west": 4.0,
            "south": 51.0,
            "east": 5.0,
            "north": 52.0,
        },
    ),
]


@pytest.mark.parametrize(["from_crs_epsg", "bbox_dict"], BBOX_TABLE)
def test_reproject_bounding_box_returns_expected_latlong_bbox(from_crs_epsg, bbox_dict):
    """Test the happy path: all these cases should return the expected BBox in WGS84."""
    west, south, east, north = bbox_dict_to_list(bbox_dict)
    new_west, new_south, new_east, new_north = reproject_bounding_box(
        west, south, east, north, from_crs=from_crs_epsg, to_crs=4326
    )
    # We only care about projected CRS and lat-long(EPSG:4623)
    # Projected CRSs are expressed in meter zo we expect accuracy up to a few meters.
    # In other worlds absulute errors of less than 10 m.
    # For lat-long we want about 0.1 seconds and 1 seconds is 1/3600 degrees,
    # so this simplifies to abs just under 1/3600 or 1/4000 = 0.0025 => 0.0001
    abs_tolerance = 0.0001 if from_crs_epsg == 4326 else 10.0

    assert new_west == approx(4.0, abs=abs_tolerance)
    assert new_east == approx(5.0, abs=abs_tolerance)
    assert new_south == approx(51.0, abs=abs_tolerance)
    assert new_north == approx(52.0, abs=abs_tolerance)

    bbox = box(west, south, east, north)
    new_bbox = project_polygon(bbox, from_crs_epsg, 4326).bounds

    assert new_bbox == approx([new_west, new_south, new_east, new_north], abs=abs_tolerance)


@pytest.mark.parametrize(["to_crs_epsg", "bbox_dict"], BBOX_TABLE)
def test_reproject_bounding_box_returns_expected_projected_bbox(
    to_crs_epsg,
    bbox_dict,
):
    """Test the happy path: all these cases should return the expected BBox in WGS84."""

    west, south, east, north = bbox_dict_to_list(bbox_dict)
    new_west, new_south, new_east, new_north = reproject_bounding_box(
        4.0, 51.0, 5.0, 52.0, from_crs=4326, to_crs=to_crs_epsg
    )

    # We only care about projected CRS and lat-long(EPSG:4623)
    # Projected CRSs are expressed in meter zo we expect accuracy up to a few meters.
    # In other worlds absulute errors of less than 10 m.
    # For lat-long we want about 0.1 seconds and 1 seconds is 1/3600 degrees,
    # so this simplifies to abs just under 1/3600 or 1/4000 = 0.0025 => 0.0001
    abs_tolerance = 0.0001 if to_crs_epsg == 4326 else 10.0

    assert new_west == approx(west, abs=abs_tolerance)
    assert new_east == approx(east, abs=abs_tolerance)
    assert new_south == approx(south, abs=abs_tolerance)
    assert new_north == approx(north, abs=abs_tolerance)

    # bbox = box(west, south, east, north)
    # new_bbox = project_polygon(bbox, from_crs=4326, to_crs=to_crs_epsg).bounds

    # assert new_bbox == approx([new_west, new_south, new_east, new_north], abs=abs_tolerance)


@pytest.mark.parametrize("to_crs_epsg", [3812, 4326])
def test_reproject_bounding_box_raises_valueerror_when_from_crs_is_empty(
    to_crs_epsg,
):
    """Verifies that argument checking works for from_crs."""

    # west, south, east, north = bbox_dict_to_list(bbox_dict)
    with pytest.raises(ValueError):
        # The coordinates to transform don't really matter for this test, only
        # the coordinate systems.
        reproject_bounding_box(
            west=1,
            south=2,
            east=3,
            north=4,
            from_crs=None,
            to_crs=to_crs_epsg,
        )


@pytest.mark.parametrize("from_crs_epsg", [3812, 4326])
def test_reproject_bounding_box_raises_valueerror_when_to_crs_is_empty(
    from_crs_epsg,
):
    """Verifies that argument checking works for to_crs."""

    with pytest.raises(ValueError):
        # The coordinates to transform don't really matter for this test, only
        # the coordinate systems.
        reproject_bounding_box(
            west=1,
            south=2,
            east=3,
            north=4,
            from_crs=from_crs_epsg,
            to_crs=None,
        )


@pytest.mark.parametrize(
    ["from_crs_epsg", "west", "south", "east", "north"],
    [
        # BBoxes below correspond to lat-long from 4E 51N to 5E 52N.
        # Belgian Lambert 2008
        # (3812, 624112.728540544, 687814.368911342, 693347.444114525, 799212.044310798),
        (3812, None, 687814.368911342, 693347.444114525, 799212.044310798),
        (3812, "illegal value", 687814.368911342, 693347.444114525, 799212.044310798),
        (3812, 624112.728540544, None, 693347.444114525, 799212.044310798),
        (3812, 624112.728540544, "illegal value", 693347.444114525, 799212.044310798),
        (3812, 624112.728540544, 687814.368911342, None, 799212.044310798),
        (3812, 624112.728540544, 687814.368911342, "illegal value", 799212.044310798),
        (3812, 624112.728540544, 687814.368911342, 693347.444114525, None),
        (3812, 624112.728540544, 687814.368911342, 693347.444114525, "illegal value"),
    ],
)
def test_reproject_bounding_box_raises_typeerror_when_bbox_components_wrong_type(
    from_crs_epsg, west, south, east, north
):
    """Verifies that argument checking works for the coordinate components."""
    with pytest.raises(TypeError):
        reproject_bounding_box(
            west=west,
            south=south,
            east=east,
            north=north,
            from_crs=from_crs_epsg,
            to_crs=4326,
        )


@pytest.mark.parametrize(
    ["from_crs_epsg", "west", "south", "east", "north"],
    [
        # BBoxes below correspond to lat-long from 4E 51N to 5E 52N.
        # Belgian Lambert 2008
        (3812, 693347.444114525, 687814.368911342, 624112.728540544, 799212.044310798),
        (4326, 5.0, 51.0, 4.0, 52.0),
        (3812, 624112.728540544, 799212.044310798, 693347.444114525, 687814.368911342),
        (4326, 4.0, 52.0, 5.0, 51.0),
    ],
)
def test_reproject_bounding_box_raises_valueerror_when_upper_and_lower_bound_swapped(
    from_crs_epsg, west, south, east, north
):
    """Verifies that argument checking works when minimum and maximum are switched around.

    This helps to detect the bounding box is using incorrect corners.
    You should transform the upper-left corner (min_x, max_y) and the
    lower-right corner (max_x, min_y).

    The other corners are really different points and that gives different
    results when you transform them.
    """
    with pytest.raises(ValueError):
        reproject_bounding_box(
            west=west,
            south=south,
            east=east,
            north=north,
            from_crs=from_crs_epsg,
            to_crs=4326,
        )


class TestGetTransform:
    @pytest.mark.parametrize("to_crs_epsg", [3812, 4326])
    def test_get_transform_raises_valueerror_when_from_crs_is_empty(self, to_crs_epsg):
        """Verifies that argument checking works for from_crs."""
        with pytest.raises(ValueError):
            get_transform(from_crs=None, to_crs=to_crs_epsg)

    @pytest.mark.parametrize("from_crs_epsg", [3812, 4326])
    def test_get_transform_raises_valueerror_when_to_crs_is_empty(self, from_crs_epsg):
        """Verifies that argument checking works for to_crs."""
        with pytest.raises(ValueError):
            get_transform(from_crs=from_crs_epsg, to_crs=None)
