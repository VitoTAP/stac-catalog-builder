"""For working with bounding boxes of the rasters, in various formats.
"""
import dataclasses as dc
from typing import Dict, List, Optional

from shapely.geometry import Polygon


def bbox_list_to_dict(bbox: List[float]) -> Dict[str, float]:
    """Convert bounding box in list format to a dictionary.

    Utility function for a common conversion.
    """
    # Unpack coordinate and ignore Z coordinates
    west, south, east, north = bbox[:4]

    return to_bbox_dict(west, south, east, north)


def bbox_dict_to_list(bbox_dict: Dict[str, float]) -> List[float]:
    """Convert bounding box in dictionary format to a list.

    Utility function for a common conversion.
    """
    b = bbox_dict
    return [b["west"], b["south"], b["east"], b["north"]]


def to_bbox_dict(west: float, south: float, east: float, north: float) -> Dict[str, float]:
    """Create a bounding box dictionary from individual XY coordinates."""
    return {
        "west": west,
        "south": south,
        "east": east,
        "north": north,
    }


def poly_from_bounds(minx, miny, maxx, maxy) -> Polygon:
    """Returns a rectangular polygon representing the bounding box."""
    return Polygon.from_bounds(minx, miny, maxx, maxy)


@dc.dataclass
class BoundingBox:
    west: float  # AKA min_x
    east: float  # AKA max_x
    south: float  # AKA min_y
    north: float  # AKA min_y
    epsg: Optional[int]

    @staticmethod
    def create_empty() -> "BoundingBox":
        return BoundingBox(0.0, 0.0, 0.0, 0.0, epsg=None)

    @property
    def min_x(self) -> float:
        return self.west

    @property
    def max_x(self) -> float:
        return self.east

    @property
    def min_y(self) -> float:
        return self.south

    @property
    def max_y(self) -> float:
        return self.north

    def to_dict(self) -> Dict[str, float]:
        return {
            "west": self.west,
            "south": self.south,
            "east": self.east,
            "north": self.north,
            "epsg": self.epsg,
        }

    def set_from_dict(self, values: Dict[str, float]) -> None:
        self.west = values["west"]
        self.east = values["east"]
        self.north = values["north"]
        self.south = values["south"]
        self.epsg = values["epsg"]

    @staticmethod
    def from_dict(values: Dict[str, float]) -> "BoundingBox":
        return BoundingBox.create_empty().set_from_dict(values)

    def to_list(self) -> List[float]:
        return [self.west, self.south, self.east, self.north]

    def set_from_list(self, bbox_list: List[float], epsg: int) -> None:
        self.west, self.south, self.east, self.north = bbox_list[:4]
        self.epsg = epsg

    @staticmethod
    def from_list(bbox_list: List[float], epsg: int) -> "BoundingBox":
        return BoundingBox.create_empty().set_from_list(bbox_list)

    def to_polygon(self) -> Polygon:
        """Returns a rectangular polygon representing the bounding box."""
        return Polygon.from_bounds(self.to_list())


@dc.dataclass
class ProjectedBoundingBox:
    """Bundles all data we want to know about the bounding box and projection."""

    bbox_lat_lon: BoundingBox
    bbox_projected: BoundingBox
    transform: Optional[List[float]]

    @property
    def projected_epsg(self) -> Optional[int]:
        if not self.bbox_projected:
            return None
        return self.bbox_projected.epsg
