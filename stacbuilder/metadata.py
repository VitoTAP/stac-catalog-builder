"""
Intermediate metadata that models the properties we actually use.

Rationale:

1) STAC times support a lot more than we really need, and this class makes it
    explicit what we actually use.
2) Implementing each conversion to STAC for every new type of input data is extra work.
    This intermediate metadata make reuse easier to do.
3) This class is deliberately simple and has little to no business logic.
4) For unit testing it is a lot simpler to instantiate Metadata objects than
    to creating fake raster files, or fake API responses from a mock of the real API.
"""
import datetime as dt
from pathlib import Path
from types import NoneType
from typing import Any, Dict, List, Optional, Tuple, Union


import rasterio
from shapely import to_wkt
from shapely.geometry import box, mapping, Polygon
from stactools.core.io import ReadHrefModifier

from openeo.util import rfc3339, normalize_crs


from stacbuilder.boundingbox import BoundingBox
from stacbuilder.pathparsers import InputPathParser
from stacbuilder.projections import reproject_bounding_box


BoundingBoxList = List[Union[float, int]]


# TODO: convert Metadata to a dataclass, and add method (or class) that fills it in from a GeoTIFF.
class Metadata:
    """Intermediate metadata that models the properties we actually use.

    TODO: Decouple: turn this into a actual dataclass and extract all rasterio stuff from the constructor.
        We should have a component that reads the file and then fills is or constructs a Metadata instance.
        Now Metadata is tied completely to GeoTIFFs and we want to use it for any
        data source as an intermediate step that makes it easier to create
        STAC items with the same subset of metadata we want to have for every data source.
    """

    # This is metadata of an asset that belongs to a STAC item, not really the metadata of one STAC item.
    # (Realizing this now when reviewing the code, this class has been roughly the same since the earliest versions of the tool)
    # We process files which correspond to assets.
    # Those assets are grouped into STAC items.
    # Also an asset can either be one band, or it can itself contain multiple bands.
    #
    # properties we want to collect:
    # - item_id
    # - path or href (How do we handle URLs?)
    # - item_type?? Or rather asset_type (to be decided and renamed)
    #       We use this to find the corresponding asset definition config in the CollectionConfig
    # - band
    # - everything to do with the date and time.
    #       datetime is the standard name (To be confirmed)
    #       But this could correspond to either the start datetime or end datetime, usually the start datetime.
    #       Often it is easier to extract year, month and day as separate parts of the path.
    #       Keep in mind some paths only contain a year, or a year and month.
    # - bounding box and everything to do with the CRS (projected CRS)
    #   - in lat-lon
    #   - as well as projected + the CRS
    #   - geometry of the bounding box
    # - raster shape (height & width in pixels)
    # - raster tags

    def __init__(
        self,
        extract_href_info: InputPathParser,
        read_href_modifier: Optional[ReadHrefModifier] = None,
    ):
        # original and modified path/href
        self._href: Optional[str] = None
        self._modified_href: Optional[str] = None

        # components to convert data
        self._read_href_modifier = read_href_modifier
        self._extract_href_info = extract_href_info

        # The raw dictionary of data extracted from the href
        self._info_from_href: Dict[str, Any] = None

        # Essential asset info: what asset is it and what STAC item does it belong to
        # What bands does it contain or correspond to.
        # TODO: support multiple bands
        # TODO: clean up how multiple assets get assigned to one STAC item of the same type.

        self._asset_id: Optional[str] = None
        """The asset ID normally corresponds to the file, unless the InputPathParser provides a value to override it."""

        self._item_id: Optional[str] = None
        """Which STAC item this asset belongs to.
        When a STAC item bundles multiple assets then the InputPathParser in extract_href_info
        must provide a value for item_id.
        However, when each asset corresponds to one STAC item then asset_id and item_id will
        be identical. So if we don't get an explicit value for item_id we assume it is the same as
        asset_id.
        """

        self._band: Optional[str] = None

        # TODO: item_type is currently a misnomer, more like an asset type.
        # We use this to find the corresponding asset definition config in the CollectionConfig
        self._asset_type: Optional[str] = None

        # Temporal extent
        # everything to do with the date + time and temporal extent this asset corresponds to.
        self._datetime = dt.datetime.utcnow()
        self._start_datetime: Optional[dt.datetime] = None
        self._end_datetime: Optional[dt.datetime] = None
        self._year: Optional[int] = None
        self._month: Optional[int] = None
        self._day: Optional[int] = None

        # Spatial extent
        self.bbox: Optional[List[float]] = None
        self.proj_bbox: Optional[List[float]] = None
        self._proj_epsg: Optional[Any] = None
        self.transform: Optional[List[float]] = None

        # raster shape in pixels
        self.shape: Optional[List[int]] = None

        # Tags in the raster.
        self.tags: List[str] = []

    @staticmethod
    def from_href(
        href: str,
        extract_href_info: InputPathParser,
        read_href_modifier: Optional[ReadHrefModifier] = None,
    ):
        meta = Metadata(extract_href_info=extract_href_info, read_href_modifier=read_href_modifier)
        meta._read_geotiff(href)
        return meta

    def _read_geotiff(self, href):
        self.href = href
        self.asset_id = Path(href).stem
        self.item_id = Path(href).stem

        if self._read_href_modifier:
            self._modified_href = self._read_href_modifier(href)
        else:
            self._modified_href = href

        with rasterio.open(self._modified_href) as dataset:
            self.proj_bbox = list(dataset.bounds)

            # reset proj_epsg
            self.proj_epsg = None
            # TODO: once this works well, integrate normalize_crs into  proj_epsg
            normalized_epsg = normalize_crs(dataset.crs)
            if normalized_epsg is not None:
                self.proj_epsg = normalized_epsg
            elif hasattr(dataset.crs, "to_epsg"):
                self.proj_epsg = dataset.crs.to_epsg()

            if not self.proj_epsg or self.proj_epsg in [4326, "EPSG:4326", "epsg:4326"]:
                self.bbox = self.proj_bbox
            else:
                west, south, east, north = self.proj_bbox[:4]
                self.bbox = reproject_bounding_box(west, south, east, north, from_crs=dataset.crs, to_crs="epsg:4326")

            self.transform = list(dataset.transform)[0:6]
            self.shape = dataset.shape
            self.tags = dataset.tags()

        self.process_href_info()

    def process_href_info(self):
        href_info = self._extract_href_info.parse(self.href)
        self._info_from_href = href_info
        for key, value in href_info.items():
            # Ignore keys that do not match any attribute.
            if hasattr(self, key):
                setattr(self, key, value)

    @property
    def href(self) -> str:
        return self._href

    @href.setter
    def href(self, value: str) -> str:
        self._href = str(value)

    @property
    def modified_href(self) -> str:
        return self._modified_href

    @modified_href.setter
    def modified_href(self, value: str) -> str:
        self._modified_href = str(value)

    @property
    def asset_id(self) -> str:
        return self._asset_id

    @asset_id.setter
    def asset_id(self, value: str) -> None:
        self._asset_id = value

    @property
    def item_id(self) -> str:
        return self._item_id

    @item_id.setter
    def item_id(self, value: str) -> None:
        self._item_id = value

    @property
    def asset_type(self) -> str:
        # Default to the band name if it is not set
        if not self._asset_type:
            return self.band
        return self._asset_type

    @asset_type.setter
    def asset_type(self, value: str) -> None:
        self._asset_type = value

    @property
    def geometry(self) -> Dict[str, Any]:
        geometry_dict: Dict[str, Any] = mapping(box(*self.bbox))
        return geometry_dict

    @property
    def proj_epsg(self) -> Union[int, None]:
        return self._proj_epsg

    @proj_epsg.setter
    def proj_epsg(self, value: Optional[int]) -> Union[int, None]:
        if not isinstance(value, (int, NoneType)):
            raise TypeError("Value of proj_epsg must be an Integer or None." + f"{type(value)=}, {value=}")
        self._proj_epsg = value

    @property
    def proj_geometry(self) -> Dict[str, Any]:
        geometry_dict: Dict[str, Any] = mapping(box(*self.proj_bbox))
        return geometry_dict

    @property
    def proj_geometry_as_wkt(self) -> str:
        return to_wkt(self.proj_geometry_shapely)

    @property
    def proj_geometry_shapely(self) -> Polygon:
        return Polygon.from_bounds(*self.proj_bbox)

    @property
    def version(self) -> str:
        return "1.0.0"

    # TODO: Remove property "band" b/c there may be > 1 band in 1 asset and we don't read them from the raster anyway.
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
            "item_id": self.item_id,
            "asset_id": self.asset_id,
            "href": self.href,
            "modified_href": self.modified_href,
            "asset_type": self.asset_type,
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


class RasterBBoxReader:
    """Reads bounding box info from a raster file format.

    TODO: this is very much unfinished untested code.
        This is an preliminary implementation, and completely UNTESTED.
        We want to extract all the raster reading stuff out of the module `metadata`
        into a separate module, and this class is the start of the process.
        It is therefore more "thinking in writing" than finished code.
    """

    @classmethod
    def from_raster_path(cls, path: Path) -> Tuple[BoundingBox, BoundingBox, List[float]]:
        with rasterio.open(path) as dataset:
            return cls.from_rasterio_dataset(dataset)

    @staticmethod
    def from_rasterio_dataset(dataset) -> Tuple[BoundingBox, BoundingBox, List[float]]:
        bbox_lat_lon = None
        bbox_projected = BoundingBox.from_list(list(dataset.bounds))
        proj_epsg = None

        # TODO: once this works well, integrate normalize_crs into  proj_epsg
        normalized_epsg = normalize_crs(dataset.crs)
        if normalized_epsg is not None:
            proj_epsg = normalized_epsg
        elif hasattr(dataset.crs, "to_epsg"):
            proj_epsg = dataset.crs.to_epsg()

        if not proj_epsg or proj_epsg in [4326, "EPSG:4326", "epsg:4326"]:
            bbox_lat_lon = bbox_projected
        else:
            west, south, east, north = bbox_projected.to_list()
            bbox_list = reproject_bounding_box(west, south, east, north, from_crs=dataset.crs, to_crs="epsg:4326")
            bbox_lat_lon = BoundingBox.from_list(bbox_list)

        transform = list(dataset.transform)[0:6]

        return bbox_lat_lon, bbox_projected, transform
