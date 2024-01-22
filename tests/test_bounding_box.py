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
        assert empty_bbox.south == 0.0
        assert empty_bbox.east == 0.0
        assert empty_bbox.north == 0.0
        assert empty_bbox.epsg is None
