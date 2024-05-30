"""
For converting bounding boxes to a different Coordinate Reference System.
"""

from functools import lru_cache
import logging
from typing import Any, Callable, List, Tuple
from shapely.geometry import polygon, box
from shapely import get_coordinates

import pyproj
import pyproj.exceptions


logger = logging.getLogger(__name__)


XYCoordinate = Tuple[float, float]
XYTransform = Callable[[float, float, bool], XYCoordinate]

def reproject_bounding_box_old(
    west: float, south: float, east: float, north: float, from_crs: Any, to_crs: Any
) -> List[float]:
    """Reproject a bounding box expressed as 4 coordinates, respectively
    the lower-left and upper-right corner or the bbox.

    :param west: AKA min_x, x-coordinate of lower-left corner
    :param south: AKA min_y, y-coordinate of lower-left corner
    :param east: AKA max_x, x-coordinate of upper-left corner
    :param north: AKA max_y, y-coordinate of upper-left corner
    :param from_crs: EPSG code of the source coordinate system
    :param to_crs: EPSG code of the source coordinate system

    :return:
        The new bounding box in the same format, list of floats in the following order:
            [new_west, new_south, new_east, new_north]

            Or in other words:
            [min_x, min_y, max_x, max_y]
            [left, bottom, top, right]
    """

    if not isinstance(west, (int, float)):
        raise TypeError(f"Argument 'west' must be a float or int but its type is {type(west)}, value={west}")
    if not isinstance(south, (int, float)):
        raise TypeError(f"Argument 'south' must be a float or int but its type is {type(south)}, value={south}")
    if not isinstance(east, (int, float)):
        raise TypeError(f"Argument 'east' must be a float or int but its type is {type(east)}, value={east}")
    if not isinstance(north, (int, float)):
        raise TypeError(f"Argument 'north' must be a float or int but its type is {type(north)}, value={north}")

    if west >= east:
        raise ValueError(f"The value of 'west' should be smaller than 'east'. {west=}, {east=}")
    if south >= north:
        raise ValueError(f"The value of 'south' should be smaller than 'north'. {south=}, {north=}")

    transform = get_transform(from_crs=from_crs, to_crs=to_crs)

    # ==========================================================================
    # CAVEAT
    # ==========================================================================
    # For a bounding box you must transform the *upper left* corner and *lower right* corner.
    #
    # Going by the order of coordinates  that most functions accept as arguments,
    # including this function here, you might think you can transform LL and UR,
    # But taking lower left and upper right will give you the wrong results!
    #
    # Reason
    # ======
    # For the transform, those points are entirely different points so the CRS
    # could project them to different points in lat-long as well.
    #
    # I put this long caveat in here because I myself got confused as well,
    # since most functions do specify a bounding box as 4 numbers, where the
    # order is lower-left then upper-right: min_x, min_y, max_x, max_y.
    # But that actually has nothing to do with which corners of the bounding box
    # you should transform.
    new_west, new_north = transform(west, north, errcheck=True)
    new_east, new_south = transform(east, south, errcheck=True)

    return [new_west, new_south, new_east, new_north]

def reproject_bounding_box(
        west: float, south: float, east: float, north: float, from_crs: Any, to_crs: Any
) -> List[float]:
    """Reproject a bounding box expressed as 4 coordinates, respectively
    the lower-left and upper-right corner or the bbox.

    :param west: AKA min_x, x-coordinate of lower-left corner
    :param south: AKA min_y, y-coordinate of lower-left corner
    :param east: AKA max_x, x-coordinate of upper-left corner
    :param north: AKA max_y, y-coordinate of upper-left corner
    :param from_crs: EPSG code of the source coordinate system
    :param to_crs: EPSG code of the source coordinate system

    :return:
        The new bounding box in the same format, list of floats in the following order:
            [new_west, new_south, new_east, new_north]

            Or in other words:
            [min_x, min_y, max_x, max_y]
            [left, bottom, top, right]
    """

    if not isinstance(west, (int, float)):
        raise TypeError(f"Argument 'west' must be a float or int but its type is {type(west)}, value={west}")
    if not isinstance(south, (int, float)):
        raise TypeError(f"Argument 'south' must be a float or int but its type is {type(south)}, value={south}")
    if not isinstance(east, (int, float)):
        raise TypeError(f"Argument 'east' must be a float or int but its type is {type(east)}, value={east}")
    if not isinstance(north, (int, float)):
        raise TypeError(f"Argument 'north' must be a float or int but its type is {type(north)}, value={north}")

    if west >= east:
        raise ValueError(f"The value of 'west' should be smaller than 'east'. {west=}, {east=}")
    if south >= north:
        raise ValueError(f"The value of 'south' should be smaller than 'north'. {south=}, {north=}")

    transform = get_transform(from_crs=from_crs, to_crs=to_crs)

    bbox = box(west, south, east, north)
    return project_polygon(geometry=bbox, from_crs=from_crs, to_crs=to_crs).bounds

def project_polygon(geometry: Any, from_crs: Any, to_crs: Any) -> Any:
    transform = get_transform(from_crs=from_crs, to_crs=to_crs)
    point_list = []
    for point in get_coordinates(geometry):
        point_list.append(transform(*point))
    return polygon.Polygon(point_list)

def get_transform(from_crs: Any, to_crs: Any) -> XYTransform:
    """Get a transform to reproject from "from_crs" to "to_crs".

    :param from_crs: EPSG code of the source coordinate system
    :param to_crs: EPSG code of the source coordinate system
    """
    transformer = _get_transformer(from_crs=from_crs, to_crs=to_crs)
    return transformer.transform

@lru_cache(maxsize=6)
def _get_transformer(from_crs: Any, to_crs: Any) -> Any:
    """Get a transformer to reproject from "from_crs" to "to_crs"..

    :param from_crs: EPSG code of the source coordinate system
    :param to_crs: EPSG code of the source coordinate system

    :return: A transformer object that can be used to transform coordinates.
    """
    if not from_crs:
        raise ValueError("Argument 'from_crs' must have a value.")
    if not to_crs:
        raise ValueError("Argument 'to_crs' must have a value.")

    try:
        transformer = pyproj.Transformer.from_crs(
            crs_from=from_crs, crs_to=to_crs, always_xy=True, allow_ballpark=True, accuracy=1.0, only_best=True
        )
    except pyproj.exceptions.CRSError:
        logger.warning(
            f"Could not find a projection transformation from CRS {from_crs=} to CRS {to_crs}.", exc_info=True
        )
        return None

    return transformer
