from typing import List

import pyproj


def reproject_bounding_box(
    west: float, south: float, east: float, north: float, from_crs: str, to_crs: str
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
    transformer = pyproj.Transformer.from_crs(crs_from=from_crs, crs_to=to_crs, always_xy=True)
    transform = transformer.transform

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
