from typing import Dict, List

from shapely.geometry import Polygon


def bbox_list_to_dict(bbox: List[float]) -> Dict[str, float]:
    """Convert bounding box in list form to a dictionary.

    Utility function for a common conversion.
    """
    # Unpack coordinate and ignore Z coordinates
    west, south, east, north = bbox[:4]

    return to_bbox_dict(west, south, east, north)


def bbox_dict_to_list(bbox_dict: Dict[str, float]) -> List[float]:
    """Convert bounding box in dictionary form to a list.

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
