import datetime as dt
from pathlib import Path
from typing import Any, Dict, List, Optional, Union


# import numpy as np
import pyproj
import rasterio

# import shapely
# import shapely.ops

from shapely.geometry import box, mapping
from stactools.core.io import ReadHrefModifier

from openeo.util import rfc3339


from stacbuilder.core import InputPathParser, reproject_bounding_box


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
                west, south, east, north = self.proj_bbox[:4]
                self.bbox = reproject_bounding_box(
                    west, south, east, north,
                    from_crs=dataset.crs, to_crs="epsg:4326"
                )
            self.transform = list(dataset.transform)[0:6]
            self.shape = dataset.shape
            self.tags = dataset.tags()

            # print(f"projected: proj_bbox={self.proj_bbox}")
            # print(f"projected CRS: {dataset.crs}")
            # print(f"{dataset.bounds=}")
            # print(f"{dataset.transform=}")
            # print(f"lat long: bbox={self.bbox}")
            # print(f"{dataset.shape=}")
            # print(f"{dataset.tags()=}")

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

    def to_dict(self, include_internal=False):
        """Convert to a dictionary for troubleshooting (output) or for other processing.

        :param include_internal:
            Include internal information for debugging, defaults to False.
        :return: A dictionary that represents the same metadata.
        """
        data = {
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
            "bbox": self.bbox,
            "proj_epsg": self.proj_epsg,
            "proj_bbox": self.proj_bbox,
            "geometry": self.geometry,
            "proj_geometry": self.proj_geometry,
        }
        # Include internal information for debugging.
        if include_internal:
            data["_info_from_href"] = self._info_from_href

        return data

    def __str__(self):
        return str(self.to_dict())


# def _reproject_bounding_box(bbox: BoundingBoxList, from_crs: str, to_crs: str) -> List[float]:
#     """
#     Reproject given bounding box dictionary

#     :param bbox: bbox dict with fields "west", "south", "east", "north"

#     :param from_crs: source CRS. Specify `None` to use the "crs" field of input bbox dict
#     :param to_crs: target CRS
#     :return: bbox dict (fields "west", "south", "east", "north", "crs")
#     """
#     if not len(bbox) == 4:
#         ValueError(f"Bounding box bbox expects exactly 4 elements. {bbox=}")

#     west, south, east, north = bbox
#     transformer = pyproj.Transformer.from_crs(crs_from=from_crs, crs_to=to_crs, always_xy=True)
#     transform = transformer.transform

#     new_west, new_south = transform(west, south, errcheck=True)
#     new_east, new_north = transform(east, north, errcheck=True)

#     return [new_west, new_south, new_east, new_north]
