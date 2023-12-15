import datetime as dt
from pathlib import Path
from typing import Any, Dict, List, Optional, Union


import rasterio
from shapely import to_wkt
from shapely.geometry import box, mapping, Polygon
from stactools.core.io import ReadHrefModifier

from openeo.util import rfc3339, normalize_crs


from stacbuilder.core import InputPathParser
from stacbuilder.projections import reproject_bounding_box


BoundingBoxList = List[Union[float, int]]


class Metadata:
    def __init__(
        self,
        href: str,
        extract_href_info: InputPathParser,
        read_href_modifier: Optional[ReadHrefModifier] = None,
    ):
        # breakpoint()
        if read_href_modifier:
            modified_href = read_href_modifier(href)
        else:
            modified_href = href
        with rasterio.open(modified_href) as dataset:
            self.proj_bbox = list(dataset.bounds)

            self._proj_epsg = None
            # TODO: once this works well, integrate normalize_crs into  proj_epsg
            normalized_epsg = normalize_crs(dataset.crs)
            if normalized_epsg is not None:
                self.proj_epsg = normalized_epsg
            elif hasattr(dataset.crs, "to_epsg"):
                self.proj_epsg = dataset.crs.to_epsg()

            if self.proj_epsg in [4326, "EPSG:4326", "epsg:4326"]:
                self.bbox = self.proj_bbox
            else:
                west, south, east, north = self.proj_bbox[:4]
                self.bbox = reproject_bounding_box(west, south, east, north, from_crs=dataset.crs, to_crs="epsg:4326")
            self.transform = list(dataset.transform)[0:6]
            self.shape = dataset.shape
            self.tags = dataset.tags()

            print(f"{href=}")
            print(f"{modified_href=}")
            print(f"projected: proj_bbox={self.proj_bbox}")
            print(f"projected CRS: {dataset.crs}")
            print(f"{dataset.bounds=}")
            print(f"{dataset.transform=}")
            print(f"lat long: bbox={self.bbox}")
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
    def proj_epsg(self) -> Union[int, None]:
        return self._proj_epsg

    @proj_epsg.setter
    def proj_epsg(self, value: Optional[int]) -> Union[int, None]:
        if not isinstance(value, (int, None)):
            raise TypeError("Value of proj_epsg must be an Integer or None." + f"{type(value)=}, {value=}")
        self._proj_epsg = value

    @property
    def proj_geometry(self) -> Dict[str, Any]:
        geometry_dict: Dict[str, Any] = mapping(box(*self.proj_bbox))
        return geometry_dict

    @property
    def proj_geometry_as_wkt(self) -> str:
        poly: Polygon = Polygon.from_bounds(*self.proj_bbox)
        return to_wkt(poly)

    @property
    def proj_geometry_shapely(self) -> Polygon:
        return Polygon.from_bounds(*self.proj_bbox)

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
            "proj_geometry_as_wkt": self.proj_geometry_as_wkt,
        }
        # Include internal information for debugging.
        if include_internal:
            data["_info_from_href"] = self._info_from_href

        return data

    def __str__(self):
        return str(self.to_dict())
