from typing import List

import pyproj


def reproject_bounding_box(
    west: float, south: float, east: float, north: float, from_crs: str, to_crs: str
) -> List[float]:
    """
    Reproject given bounding box dictionary

    :param bbox: bbox dict with fields "west", "south", "east", "north"

    :param from_crs: source CRS. Specify `None` to use the "crs" field of input bbox dict
    :param to_crs: target CRS
    :return: bbox dict (fields "west", "south", "east", "north", "crs")
    """
    transformer = pyproj.Transformer.from_crs(crs_from=from_crs, crs_to=to_crs, always_xy=True)
    transform = transformer.transform

    new_west, new_south = transform(west, south, errcheck=True)
    new_east, new_north = transform(east, north, errcheck=True)

    return [new_west, new_south, new_east, new_north]
