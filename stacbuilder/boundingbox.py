"""For working with bounding boxes of the rasters, in various formats.
"""
import dataclasses as dc
from typing import Dict, List, Optional

from shapely.geometry import Polygon


def bbox_list_to_dict(bbox: List[float]) -> Dict[str, float]:
    """Convert bounding box in list format to a dictionary.

    Utility function for a common conversion.
    The result does not include the CRS (EPSG code), only coordinates.
    """
    # Unpack coordinate and ignore Z coordinates
    west, south, east, north = bbox[:4]

    return to_bbox_dict(west, south, east, north)


def bbox_dict_to_list(bbox_dict: Dict[str, float]) -> List[float]:
    """Convert bounding box in dictionary format to a list.

    Utility function for a common conversion.
    The result does not include the CRS (EPSG code), only coordinates.
    """
    b = bbox_dict
    return [b["west"], b["south"], b["east"], b["north"]]


def to_bbox_dict(west: float, south: float, east: float, north: float) -> Dict[str, float]:
    """Create a bounding box dictionary from individual XY coordinates.

    Utility function for a common conversion.
    The result does not include the CRS (EPSG code), only coordinates.
    """
    return {
        "west": west,
        "south": south,
        "east": east,
        "north": north,
    }


# def polygon_from_bounds(minx, miny, maxx, maxy) -> Polygon:
#     """
#     Returns a rectangular polygon representing the bounding box.

#     Utility function for a common conversion.
#     The resulting Polygon does not have any information about the CRS (EPSG code), only coordinates.
#     """
#     return Polygon.from_bounds(minx, miny, maxx, maxy)


@dc.dataclass
class BoundingBox:
    """Bounding box in a GIS coordinate reference system.

    We assume that it is a CRS that is has an EPSG code, since these are the
    ones we support in openEO.

    For the names of the coordinates we choose the geographic names, i.e.
    west, east for the (X-axis), and south and north for the Y-axis, rather
    than x_min, x_max, y_min and y_max.
    This is simply because that terminology is more common throughout the
    openEO software, even though in some special cases the x_min/max y_min/max
    names may be more accurate.

    For ease of use do provide x_min, x_max, y_min and y_max as read-only properties.
    You could regards these as an alias for the other properties.
    """

    west: float  # AKA min_x
    south: float  # AKA min_y
    east: float  # AKA max_x
    north: float  # AKA min_y
    epsg: Optional[int]

    @staticmethod
    def create_empty() -> "BoundingBox":
        return BoundingBox(0.0, 0.0, 0.0, 0.0, epsg=None)

    @property
    def min_x(self) -> float:
        """Mininum X coordinate, also known as "west"."""
        return self.west

    @property
    def max_x(self) -> float:
        """Maximum X coordinate, also known as "east"."""
        return self.east

    @property
    def min_y(self) -> float:
        """Mininum Y coordinate, also known as "south"."""
        return self.south

    @property
    def max_y(self) -> float:
        """Maximum Y coordinate, also known as "north"."""
        return self.north

    def to_dict(self) -> Dict[str, float]:
        """Convert coordinates to the standard W,S,E,N dictionary format."""
        return {
            "west": self.west,
            "south": self.south,
            "east": self.east,
            "north": self.north,
            "epsg": self.epsg,
        }

    # TODO: method name could be better
    def set_from_dict(self, values: Dict[str, float]) -> None:
        """Take the new coordinate values from a dictionary."""
        self.west = values["west"]
        self.south = values["south"]
        self.east = values["east"]
        self.north = values["north"]
        self.epsg = values["epsg"]

    @staticmethod
    def from_dict(values: Dict[str, float]) -> "BoundingBox":
        """Create an instance using the values from a dictionary.

        Utility method because this is a common case.
        """
        bbox = BoundingBox.create_empty()
        bbox.set_from_dict(values)
        return bbox

    def to_list(self) -> List[float]:
        """Convert coordinates to the standard W,S,E,N list format."""
        return [self.west, self.south, self.east, self.north]

    # TODO: method name could be better
    def set_from_list(self, bbox_list: List[float], epsg: int) -> None:
        """Take the new coordinate values from a list that has the order W,S,E,N ."""
        self.west, self.south, self.east, self.north = bbox_list[:4]
        self.epsg = epsg

    @staticmethod
    def from_list(bbox_list: List[float], epsg: int) -> "BoundingBox":
        """Create an instance using the values from a list that has the order W,S,E,N ."""
        bbox = BoundingBox.create_empty()
        bbox.set_from_list(bbox_list, epsg=epsg)
        return bbox

    def to_polygon(self) -> Polygon:
        """Returns a rectangular polygon representing the bounding box."""
        return Polygon.from_bounds(*self.to_list())
