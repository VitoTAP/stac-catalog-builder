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


import dateutil.parser
import rasterio
from shapely import to_wkt
from shapely.geometry import box, mapping, Polygon
from stactools.core.io import ReadHrefModifier

from openeo.util import normalize_crs


from stacbuilder.boundingbox import BoundingBox
from stacbuilder.pathparsers import InputPathParser
from stacbuilder.projections import reproject_bounding_box


BoundingBoxList = List[Union[float, int]]


# TODO: convert Metadata to a dataclass, and add method (or class) that fills it in from a GeoTIFF.
class AssetMetadata:
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
    # - asset_type:
    #       We use this to find the corresponding asset definition config in the CollectionConfig
    # - band(s)
    # - everything to do with the date and time.
    #       "datetime" is the standard name (To be confirmed)
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
        self._datetime = None
        self._start_datetime: Optional[dt.datetime] = None
        self._end_datetime: Optional[dt.datetime] = None
        self._year: Optional[int] = None
        self._month: Optional[int] = None
        self._day: Optional[int] = None

        # TOGGLE tot compare new implementation to old in tests while we are refactoring.
        # TODO: remove self._use_new_bbox_method, this toggle is a hack for testing the refactored version.
        # Refactoring to use cleaner, easier to unit test BoundingBox class
        self._use_new_bbox_method = True

        # Spatial extent, the old properties that we will replace. Refactoring in progress.
        self._bbox_list: Optional[List[float]] = None
        self._proj_bbox: Optional[List[float]] = None
        self._proj_epsg: Optional[Any] = None
        # new properties for the bounding boxes.
        self._bbox_lat_lon: Optional[BoundingBox] = None
        self._bbox_projected: Optional[BoundingBox] = None

        # Affine transform as the raster file states it, in case we need it for CRS conversions.
        self.transform: Optional[List[float]] = None

        # Raster shape in pixels
        self.shape: Optional[List[int]] = None

        # Tags in the raster.
        self.tags: List[str] = []

    @staticmethod
    def from_href(
        href: str,
        extract_href_info: InputPathParser,
        read_href_modifier: Optional[ReadHrefModifier] = None,
    ):
        meta = AssetMetadata(extract_href_info=extract_href_info, read_href_modifier=read_href_modifier)
        meta._read_geotiff(href)
        return meta

    def _read_geotiff(self, href):
        self.href = href
        self.asset_id = Path(href).stem
        self.item_id = Path(href).stem
        self.datetime = dt.datetime.utcnow()

        if self._read_href_modifier:
            self._modified_href = self._read_href_modifier(href)
        else:
            self._modified_href = href

        with rasterio.open(self._modified_href) as dataset:
            self.shape = dataset.shape
            self.tags = dataset.tags()

            if self._use_new_bbox_method:
                self._bbox_lat_lon, self._bbox_projected, self._transform = RasterBBoxReader.from_rasterio_dataset(
                    dataset
                )
                self._bbox_list = self._bbox_lat_lon.to_list()
                self._proj_bbox = self._bbox_projected.to_list()
                self._proj_epsg = self._bbox_projected.epsg
            else:
                self._proj_bbox = list(dataset.bounds)

                # reset proj_epsg
                self._proj_epsg = None
                # TODO: once this works well, integrate normalize_crs into  proj_epsg
                normalized_epsg = normalize_crs(dataset.crs)
                if normalized_epsg is not None:
                    self._proj_epsg = normalized_epsg
                elif hasattr(dataset.crs, "to_epsg"):
                    self._proj_epsg = dataset.crs.to_epsg()

                if not self._proj_epsg or self._proj_epsg in [4326, "EPSG:4326", "epsg:4326"]:
                    self._bbox_list = self._proj_bbox
                else:
                    west, south, east, north = self._proj_bbox[:4]
                    self._bbox_list = reproject_bounding_box(
                        west, south, east, north, from_crs=dataset.crs, to_crs="epsg:4326"
                    )

                self.transform = list(dataset.transform)[0:6]

        self.process_href_info()

    def process_href_info(self):
        href_info = self._extract_href_info.parse(self.href)
        self._info_from_href = href_info
        for key, value in href_info.items():
            # Ignore keys that do not match any attribute.
            if hasattr(self, key):
                setattr(self, key, value)

    @property
    def version(self) -> str:
        # TODO: make name more specific: check what this version is about (STAC version most likely)
        return "1.0.0"

    @property
    def href(self) -> Optional[str]:
        return self._href

    @href.setter
    def href(self, value: str) -> Optional[str]:
        self._href = str(value)

    @property
    def modified_href(self) -> Optional[str]:
        return self._modified_href

    @modified_href.setter
    def modified_href(self, value: str) -> str:
        self._modified_href = str(value)

    @property
    def asset_id(self) -> Optional[str]:
        return self._asset_id

    @asset_id.setter
    def asset_id(self, value: str) -> None:
        self._asset_id = value

    @property
    def item_id(self) -> Optional[str]:
        return self._item_id

    @item_id.setter
    def item_id(self, value: str) -> None:
        self._item_id = value

    # TODO: Remove property "band" b/c there may be > 1 band in 1 asset and we don't read them from the raster anyway.
    @property
    def band(self) -> Optional[str]:
        return self._band

    @band.setter
    def band(self, value: str) -> None:
        self._band = value

    @property
    def asset_type(self) -> Optional[str]:
        # Default to the band name if it is not set
        # TODO: remove this fallback, and going to remove band as a property as well.
        if not self._asset_type:
            return self.band
        return self._asset_type

    @asset_type.setter
    def asset_type(self, value: str) -> None:
        self._asset_type = value

    @property
    def bbox_lat_lon(self) -> Optional[BoundingBox]:
        return self._bbox_lat_lon

    @bbox_lat_lon.setter
    def bbox_lat_lon(self, bbox: BoundingBox) -> None:
        self._bbox_lat_lon = bbox

    @property
    def bbox_as_list(self) -> Optional[List[float]]:
        if self._use_new_bbox_method:
            if not self._bbox_lat_lon:
                return None
            return self._bbox_lat_lon.to_list()

        return self._bbox_list

    @property
    def bbox_projected(self) -> Optional[BoundingBox]:
        return self._bbox_projected

    @bbox_projected.setter
    def bbox_projected(self, bbox: BoundingBox) -> None:
        self._bbox_projected = bbox

    @property
    def proj_bbox_as_list(self) -> Optional[List[float]]:
        # TODO: [decide] convert this RO property to a method or not?
        if self._use_new_bbox_method:
            if not self._bbox_projected:
                return None
            return self._bbox_projected.to_list()

        return self._proj_bbox

    @property
    def proj_epsg(self) -> Optional[int]:
        if self._use_new_bbox_method:
            if not self._bbox_projected:
                return None
            return self._bbox_projected.epsg

        return self._proj_epsg

    @proj_epsg.setter
    def proj_epsg(self, value: Optional[int]) -> Optional[int]:
        # TODO: remove setter for epsg, should become a readonly property that returns self._bbox_projected.epsg.
        if not isinstance(value, (int, NoneType)):
            raise TypeError("Value of proj_epsg must be an Integer or None." + f"{type(value)=}, {value=}")
        self._proj_epsg = value

    @property
    def geometry_as_dict(self) -> Optional[Dict[str, Any]]:
        # TODO: [decide] convert this RO property to a method or not?
        if self._use_new_bbox_method:
            if not self._bbox_lat_lon:
                return None
            return self._bbox_lat_lon.as_geometry_dict()

        geometry_dict: Dict[str, Any] = mapping(box(*self._bbox_list))
        return geometry_dict

    @property
    def proj_geometry_as_dict(self) -> Optional[Dict[str, Any]]:
        # TODO: [decide] convert this RO property to a method or not?
        if self._use_new_bbox_method:
            if not self._bbox_projected:
                return None
            return self._bbox_projected.as_geometry_dict()

        geometry_dict: Dict[str, Any] = mapping(box(*self._proj_bbox))
        return geometry_dict

    @property
    def proj_geometry_as_wkt(self) -> Optional[str]:
        # TODO: [decide] convert this RO property to a method or not?
        if self._use_new_bbox_method:
            if not self._bbox_projected:
                return None
            return self._bbox_projected.as_wkt()

        return to_wkt(self.proj_bbox_as_polygon)

    @property
    def proj_bbox_as_polygon(self) -> Optional[Polygon]:
        # TODO: [decide] convert this RO property to a method or not?
        # TODO: method name could be better
        if self._use_new_bbox_method:
            if not self._bbox_projected:
                return None
            return self._bbox_projected.as_polygon()

        return Polygon.from_bounds(*self._proj_bbox)

    @property
    def datetime(self) -> Optional[dt.datetime]:
        return self._datetime

    @datetime.setter
    def datetime(self, value) -> None:
        self._datetime = self.check_datetime(value)

    @classmethod
    def check_datetime(cls, value) -> dt.datetime:
        if isinstance(value, dt.datetime):
            return value

        if isinstance(value, dt.date):
            return cls.convert_date_to_datetime(value)

        if isinstance(value, str):
            # if not value.endswith("Z") and not "+" in value:
            #     converted_value = value + "Z"
            # else:
            #     converted_value = value
            converted_value = dateutil.parser.parse(value)
            if not isinstance(converted_value, dt.datetime):
                return cls.convert_date_to_datetime(converted_value)
            else:
                return converted_value

        raise TypeError(f"Can not convert this time to datetime: type={type(value)}, {value=}")

    @staticmethod
    def convert_date_to_datetime(value: dt.date) -> dt.datetime:
        return dt.datetime(value.year, value.month, value.day, 0, 0, 0, tzinfo=dt.UTC)

    @property
    def start_datetime(self) -> Optional[dt.datetime]:
        return self._start_datetime

    @start_datetime.setter
    def start_datetime(self, value: dt.datetime) -> None:
        self._start_datetime = self.check_datetime(value)

    @property
    def end_datetime(self) -> Optional[dt.datetime]:
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
            "bbox": self.bbox_as_list,
            "proj_epsg": self.proj_epsg,
            "proj_bbox": self._proj_bbox,
            "geometry": self.geometry_as_dict,
            "proj_geometry": self.proj_geometry_as_dict,
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
        bbox_projected = None
        proj_epsg = None

        # TODO: once this works well, integrate normalize_crs into  proj_epsg
        normalized_epsg = normalize_crs(dataset.crs)
        if normalized_epsg is not None:
            proj_epsg = normalized_epsg
        elif hasattr(dataset.crs, "to_epsg"):
            proj_epsg = dataset.crs.to_epsg()

        bbox_projected = BoundingBox.from_list(list(dataset.bounds), epsg=proj_epsg)

        if not proj_epsg or proj_epsg in [4326, "EPSG:4326", "epsg:4326"]:
            bbox_lat_lon = bbox_projected
        else:
            west, south, east, north = bbox_projected.to_list()
            bbox_list = reproject_bounding_box(west, south, east, north, from_crs=dataset.crs, to_crs="epsg:4326")
            bbox_lat_lon = BoundingBox.from_list(bbox_list, epsg=4326)

        transform = list(dataset.transform)[0:6]

        return bbox_lat_lon, bbox_projected, transform
