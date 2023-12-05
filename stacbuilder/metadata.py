import os
import datetime as dt
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Union


import numpy as np
import pyproj
import rasterio
import shapely
import shapely.ops

from pystac import Asset, MediaType
from pystac.utils import make_absolute_href, str_to_datetime
from shapely.geometry import box, mapping
from stactools.core.io import ReadHrefModifier


from openeo.util import rfc3339


from stacbuilder.core import InputPathParser


BoundingBoxList = List[Union[float, int]]


class Metadata:
    def __init__(
        self,
        href: str,
        extract_href_info: InputPathParser,
        read_href_modifier: Optional[ReadHrefModifier] = None,
    ):

        if read_href_modifier:
            modified_href = read_href_modifier(href)
        else:
            modified_href = href
        with rasterio.open(modified_href) as dataset:
            self.proj_bbox = list(dataset.bounds)
            self.proj_epsg = dataset.crs
            if self.proj_epsg in [4326, "EPSG:4326", "epsg:4326"]:
                self.bbox = self.proj_bbox
            else:
                self.bbox = _reproject_bounding_box(self.proj_bbox, from_crs=dataset.crs, to_crs="epsg:4326")
            self.transform = list(dataset.transform)[0:6]
            self.shape = dataset.shape
            self.tags = dataset.tags()

        self.href = href
        self._item_id = Path(href).stem
        self._item_type = None
        self._band = None

        self._datetime = dt.datetime.utcnow()
        self._start_datetime = None
        self._end_datetime = None
        self._year = None
        self._month = None
        self._day = None

        self._extract_href_info = extract_href_info
        self.process_href_info()

    def process_href_info(self):
        href_info = self._extract_href_info.parse(self.href)
        self._info_from_href = href_info
        for key, value in href_info.items():
            # Ignore keys that do not match any attribute.
            if hasattr(self, key):
                setattr(self, key, value)

    @property
    def item_id(self) -> str:
        return self._item_id

    @item_id.setter
    def item_id(self, value: str) -> None:
        self._item_id = value

    @property
    def item_type(self) -> str:
        if not self._item_type:
            return self.band
        return self._item_type

    @item_type.setter
    def item_type(self, value: str) -> None:
        self._item_type = value

    @property
    def geometry(self) -> Dict[str, Any]:
        geometry_dict: Dict[str, Any] = mapping(box(*self.bbox))
        return geometry_dict

    @property
    def proj_geometry(self) -> Dict[str, Any]:
        geometry_dict: Dict[str, Any] = mapping(box(*self.proj_bbox))
        return geometry_dict

    @property
    def version(self) -> str:
        return "1.0.0"

    @property
    def band(self) -> str:
        return self._band

    @band.setter
    def band(self, value: str) -> None:
        self._band = value

    @property
    def datetime(self) -> dt.datetime:
        return self._datetime

    @datetime.setter
    def datetime(self, value) -> None:
        self._datetime = self.check_datetime(value)

    def check_datetime(self, value) -> dt.datetime:
        if isinstance(value, dt.datetime):
            return value

        if isinstance(value, dt.date):
            return self.convert_date_to_datetime(value)

        if isinstance(value, str):
            # if not value.endswith("Z") and not "+" in value:
            #     converted_value = value + "Z"
            # else:
            #     converted_value = value
            converted_value = rfc3339.parse_date_or_datetime(value)
            if isinstance(converted_value, dt.date):
                return self.convert_date_to_datetime(converted_value)
            else:
                return converted_value

        raise TypeError(f"Can not convert this time to datetime: type={type(value)}, {value=}")

    def convert_date_to_datetime(self, value) -> dt.datetime:
        return dt.datetime(value.year, value.month, value.day, 0, 0, 0, tzinfo=dt.UTC)

    @property
    def start_datetime(self) -> dt.datetime:
        return self._start_datetime

    @start_datetime.setter
    def start_datetime(self, value: dt.datetime) -> None:
        self._start_datetime = self.check_datetime(value)

    @property
    def end_datetime(self) -> dt.datetime:
        return self._end_datetime

    @end_datetime.setter
    def end_datetime(self, value: dt.datetime) -> None:
        self._end_datetime = self.check_datetime(value)

    @property
    def year(self) -> Optional[int]:
        return self._year

    @year.setter
    def year(self, value: int) -> None:
        self._year = value

    @property
    def month(self) -> Optional[int]:
        return self._month

    @month.setter
    def month(self, value: int) -> None:
        self._month = value

    @property
    def day(self) -> Optional[int]:
        return self._day

    @day.setter
    def day(self, value: int) -> None:
        self._day = value

    def to_dict(self):
        return {
            "itemId": self.item_id,
            "href": self.href,
            "item_type": self.item_type,
            "band": self.band,
            "datetime": self.datetime,
            "start_datetime": self.start_datetime,
            "end_datetime": self.end_datetime,
            "year": self.year,
            "month": self.month,
            "day": self.day,
            "geometry": self.geometry,
            "proj_geometry": self.proj_geometry,
            "_info_from_href": self._info_from_href,
        }

    def __str__(self):
        return str(self.to_dict())


def _reproject_bounding_box(bbox: BoundingBoxList, from_crs: str, to_crs: str) -> List[float]:
    """
    Reproject given bounding box dictionary

    :param bbox: bbox dict with fields "west", "south", "east", "north"

    :param from_crs: source CRS. Specify `None` to use the "crs" field of input bbox dict
    :param to_crs: target CRS
    :return: bbox dict (fields "west", "south", "east", "north", "crs")
    """
    if not len(bbox) == 4:
        ValueError(f"Bounding box bbox expects exactly 4 elements. {bbox=}")

    west, south, east, north = bbox
    box = shapely.geometry.box(west, south, east, north)
    transform = _get_crs_transformer(from_crs=from_crs, to_crs=to_crs)
    # reprojected = shapely.transform(box, transform, include_z=False)
    reprojected = shapely.ops.transform(transform, box)

    return list(reprojected.bounds)


# TODO: fix mypy warnings about the types for the transformer.
# For now this will have to do:

XYArray = np.ndarray

TransformerFunction = Callable[[XYArray], XYArray]


def _get_crs_transformer(from_crs: str, to_crs: str = "EPSG:4326") -> TransformerFunction:
    transformer = pyproj.Transformer.from_crs(crs_from=from_crs, crs_to=to_crs, always_xy=True)
    return transformer.transform

    # def transform_xy(xy_array: XYArray) -> XYArray:  # type: ignore[type-arg]
    #     # Does not work. need to transform each rows (containing x & y) into a new np.ndarray
    #     rows = []
    #     for i in range(xy_array.shape[0]):
    #         rows.append(transformer.transform(xx=xy_array[i, 0], yy=xy_array[i, 1]))

    #     array = np.array(rows)
    #     return array.reshape(xy_array.shape)

    def transform_xy(x: float, y: float) -> Tuple[float, float]:  # type: ignore[type-arg]
        # Does not work. need to transform each rows (containing x & y) into a new np.ndarray
        return transformer.transform(xx=x, yy=y)

    print(transform_xy.__annotations__)
    return transform_xy


# Okay
