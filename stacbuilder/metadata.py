"""
Intermediate asset metadata that models the properties we actually use.

Rationale:

1) STAC times support a lot more than we really need, and this class makes it
    explicit what we actually use.
2) Implementing each conversion to STAC for every new type of input data is extra work.
    This intermediate metadata make reuse easier to do.
3) This class is deliberately simple and has little to no business logic.
4) For unit testing it is a lot simpler to instantiate Metadata objects than
    to creating fake raster files, or fake API responses from a mock of the real API.
"""

from dataclasses import dataclass
import datetime as dt
from pathlib import Path
from types import NoneType
from typing import Any, Dict, List, Optional, Tuple, Union


import dateutil.parser
import geopandas as gpd
import numpy as np
import pandas as pd
from shapely.geometry import Polygon
from pystac.media_type import MediaType

from stacbuilder.boundingbox import BoundingBox
from stacbuilder.pathparsers import InputPathParser


BoundingBoxList = List[Union[float, int]]


@dataclass
class BandMetadata:
    """This class is temporary, will be refactored.
    Right now the focus is just to get the data out of rasterio.
    We do want a data structure that has everything we need for these extensions:
    eo:bands and raster:bands.

    The inputs from HRL VPP (terracatalogueclient) are very different.
    Probably will need to add a propery "bands" to AssetMetadata.
    """

    data_type: np.dtype
    nodata: Any
    index: int
    units: Optional[str] = None

    def to_dict(self) -> dict[str, Any]:
        result = {
            "data_type": self.data_type,
            "nodata": self.nodata,
            "index": self.index,
        }
        if self.units:
            result["units"] = self.units
        return result


# TODO: Eliminate RasterMetadata if possible and keep only the bands.
@dataclass
class RasterMetadata:
    shape: Tuple[int, int]
    bands: List[BandMetadata]

    def to_dict(self) -> dict[str, Any]:
        return {
            "shape": self.shape,
            "bands": [b.to_dict() for b in self.bands],
        }


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
    #    TODO: Do we use raster tags in the STAC Items or collections? If not, can we remove this?
    # - Metadata from the raster about the bands,
    #       for the EO and Raster STAC extensions (eo:bands and raster:bands)
    # TODO: will probably also need the spatial resolution.

    # TODO: more explicit handling of data extracted from path or href, should be a class perhaps.
    PROPS_FROM_HREFS = [
        "href",
        "original_href",
        "item_id",
        "asset_id",
        "asset_type",
        "datetime",
        "start_datetime",
        "end_datetime",
    ]

    def __init__(
        self,
        extract_href_info: Optional[InputPathParser] = None,
    ):
        # original and modified path/href
        self._href: Optional[str] = None
        self._original_href: Optional[str] = None

        # Very often the input will be a (Posix) path to an asset file, for now only GeoTIFFs.
        # When we start to support using URLs to convert those assets to STAC items, then
        # `self._asset_path` could be None, or it could be set to the local path that the
        # openEO server needs to access.
        self._asset_path: Optional[Path] = None

        # components to convert data
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

        # TODO: item_type is currently a misnomer, more like an asset type.
        # We use this to find the corresponding asset definition config in the CollectionConfig
        self._asset_type: Optional[str] = None

        # Temporal extent
        # everything to do with the date + time and temporal extent this asset corresponds to.
        self._datetime = None
        self._start_datetime: Optional[dt.datetime] = None
        self._end_datetime: Optional[dt.datetime] = None

        # The bounding boxes, in latitude-longitude and also the projected version.
        self._bbox_lat_lon: Optional[BoundingBox] = None
        self._bbox_projected: Optional[BoundingBox] = None

        # The geometry is sometimes provided by the source data, and its coordinates
        # might have more decimals than we get in the bounding box.
        # TODO How to deal with this duplicate data in a consistent and understandable way?
        self._geometry_lat_lon: Optional[Polygon] = None

        # Affine transform as the raster file states it, in case we need it for CRS conversions.
        self.transform: Optional[List[float]] = None

        # Raster shape in pixels
        self.shape: Optional[List[int]] = None

        # file size, corresponds to file:size from FileInfo STAC extension
        self.file_size: Optional[int] = None

        # Tags in the raster.
        self.tags: List[str] = []

        self.raster_metadata: Optional[RasterMetadata] = None

        self.title: Optional[str] = None
        self.collection_id: Optional[str] = None
        self.tile_id: Optional[str] = None
        self.item_href: Optional[str] = None
        self.media_type: Optional[MediaType] = None

        # TODO: add some properties that need to trickle up to the collection level, or not?
        # These properties are really at the collection level, but in HRL VPP
        # we might have to extract them from the products.
        # Have to figure out what is the best way to handle this.
        # self.platforms: Optional[List[str]] = None
        # self.instruments: Optional[List[str]] = None
        # self.missions: Optional[List[str]] = None

    def process_href_info(self):
        href_info = self._extract_href_info.parse(self.href)
        self._info_from_href = href_info
        for key, value in href_info.items():
            # Ignore keys that do not match any attribute that should come from the href.
            if key in self.PROPS_FROM_HREFS:
                setattr(self, key, value)
            # if hasattr(self, key):
            #     setattr(self, key, value)

    @property
    def version(self) -> str:
        # TODO: make name more specific: check what this version is about (STAC version most likely)
        return "1.0.0"

    @property
    def href(self) -> Optional[str]:
        return self._href

    @href.setter
    def href(self, value: str) -> None:
        self._href = str(value)

    @property
    def original_href(self) -> Optional[str]:
        return self._original_href

    @original_href.setter
    def original_href(self, value: str) -> None:
        self._original_href = str(value)

    @property
    def asset_path(self) -> Optional[Path]:
        return self._asset_path

    @asset_path.setter
    def asset_path(self, value: Union[Path, str]) -> None:
        self._asset_path = Path(value) if value else None

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

    @property
    def asset_type(self) -> Optional[str]:
        # Default to the band name if it is not set
        # TODO: remove this fallback, and going to remove band as a property as well.
        # if not self._asset_type:
        #     return self.band
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
        if not self._bbox_lat_lon:
            return None
        return self._bbox_lat_lon.to_list()

    @property
    def bbox_projected(self) -> Optional[BoundingBox]:
        return self._bbox_projected

    @bbox_projected.setter
    def bbox_projected(self, bbox: BoundingBox) -> None:
        self._bbox_projected = bbox

    @property
    def proj_bbox_as_list(self) -> Optional[List[float]]:
        # TODO: [decide] convert this RO property to a method or not?
        if not self._bbox_projected:
            return None
        return self._bbox_projected.to_list()

    @property
    def proj_epsg(self) -> Optional[int]:
        if not self._bbox_projected:
            return None
        return self._bbox_projected.epsg

    @proj_epsg.setter
    def proj_epsg(self, value: Optional[int]) -> None:
        # TODO: remove setter for epsg, should become a readonly property that returns self._bbox_projected.epsg.
        if not isinstance(value, (int, NoneType)):
            raise TypeError("Value of proj_epsg must be an Integer or None." + f"{type(value)=}, {value=}")
        self._proj_epsg = value

    @property
    def geometry_lat_lon(self) -> Polygon:
        # If a value was not set explicitly, then derive it from
        # bbox_lat_lon, if possible.
        if not self._geometry_lat_lon and self._bbox_lat_lon:
            self._geometry_lat_lon = self._bbox_lat_lon.as_polygon()
        return self._geometry_lat_lon

    @geometry_lat_lon.setter
    def geometry_lat_lon(self, geometry: Polygon):
        if not isinstance(geometry, (Polygon, NoneType)):
            raise TypeError(
                "geometry must be of type shapely.geometry.Polygon or else be None "
                + f"but the type is {type(geometry)}, {geometry=}"
            )
        self._geometry_lat_lon = geometry

    @property
    def geometry_as_dict(self) -> Optional[Dict[str, Any]]:
        # TODO: [decide] convert this RO property to a method or not?
        if not self._bbox_lat_lon:
            return None
        return self._bbox_lat_lon.as_geometry_dict()

    @property
    def proj_geometry_as_dict(self) -> Optional[Dict[str, Any]]:
        # TODO: [decide] convert this RO property to a method or not?
        if not self._bbox_projected:
            return None
        return self._bbox_projected.as_geometry_dict()

    @property
    def proj_geometry_as_wkt(self) -> Optional[str]:
        # TODO: [decide] convert this RO property to a method or not?
        if not self._bbox_projected:
            return None
        return self._bbox_projected.as_wkt()

    @property
    def proj_bbox_as_polygon(self) -> Optional[Polygon]:
        # TODO: [decide] convert this RO property to a method or not?
        # TODO: method name could be better
        if not self._bbox_projected:
            return None
        return self._bbox_projected.as_polygon()

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
        if not self._datetime:
            return None
        return self._datetime.year

    @property
    def month(self) -> Optional[int]:
        if not self._datetime:
            return None
        return self._datetime.month

    @property
    def day(self) -> Optional[int]:
        if not self._datetime:
            return None
        return self._datetime.day

    def to_dict(self, include_internal=False) -> Dict[str, Any]:
        """Convert to a dictionary for troubleshooting (output) or for other processing.

        :param include_internal:
            Include internal information for debugging, defaults to False.
        :return: A dictionary that represents the same metadata.
        """
        data = {
            "asset_id": self.asset_id,
            "item_id": self.item_id,
            "collection_id": self.collection_id,
            "tile_id": self.tile_id,
            "title": self.title,
            "href": self.href,
            "original_href": self.original_href,
            "item_href": self.item_href,
            "asset_path": self.asset_path,
            "asset_type": self.asset_type,
            "media_type": self.media_type,
            "datetime": self.datetime,
            "start_datetime": self.start_datetime,
            "end_datetime": self.end_datetime,
            "shape": self.shape,
            "tags": self.tags,
            "bbox_lat_lon": self.bbox_lat_lon.to_dict() if self.bbox_lat_lon else None,
            "bbox_projected": self.bbox_projected.to_dict() if self.bbox_projected else None,
            "geometry_lat_lon": self.geometry_lat_lon,
            "raster_metadata": self.raster_metadata.to_dict() if self.raster_metadata else None,
            "file_size": self.file_size,
        }
        # Include internal information for debugging.
        if include_internal:
            data["_info_from_href"] = self._info_from_href

        return data

    @staticmethod
    def mime_to_media_type(mime_string: str) -> Optional[MediaType]:
        """Get the pystac.mediatype.MediaType that is equivalent to the MIME string, if known.

        Returns None if the string does is not one of the enum members in MediaType.
        """
        media_type_map = {mt.value: mt for mt in MediaType}
        return media_type_map.get(mime_string)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "AssetMetadata":
        metadata = AssetMetadata()
        metadata.asset_id = cls.__get_str_from_dict("asset_id", data)
        metadata.item_id = cls.__get_str_from_dict("item_id", data)
        metadata.collection_id = cls.__get_str_from_dict("collection_id", data)
        metadata.tile_id = cls.__get_str_from_dict("tile_id", data)
        metadata.title = cls.__get_str_from_dict("title", data)

        metadata.href = cls.__get_str_from_dict("href", data)
        metadata.original_href = cls.__get_str_from_dict("original_href", data)
        metadata.item_href = cls.__get_str_from_dict("item_href", data)
        metadata.asset_path = cls.__as_type_from_dict("asset_path", Path, data)

        metadata.asset_type = cls.__get_str_from_dict("asset_type", data)
        metadata.file_size = cls.__as_type_from_dict("file_size", int, data)

        media_type = data.get("media_type")
        metadata.media_type = cls.mime_to_media_type(media_type)

        metadata.datetime = cls.__as_type_from_dict("datetime", pd.Timestamp.to_pydatetime, data)
        metadata.start_datetime = cls.__as_type_from_dict("start_datetime", pd.Timestamp.to_pydatetime, data)
        metadata.end_datetime = cls.__as_type_from_dict("end_datetime", pd.Timestamp.to_pydatetime, data)

        metadata.shape = data.get("shape")
        metadata.tags = data.get("tags")

        bbox_dict = data.get("bbox_lat_lon")
        if bbox_dict:
            bbox = BoundingBox.from_dict(bbox_dict)
            assert bbox.epsg in (4326, None)
            metadata.bbox_lat_lon = bbox
        else:
            metadata.bbox_lat_lon = None

        # geom_dict = data.get("geometry")
        # from shapely import from_geojson
        # import json
        # metadata.geometry_lat_lon = from_geojson(json.dumps(geom_dict))
        metadata.geometry_lat_lon = data.get("geometry_lat_lon")

        proj_bbox_dict = data.get("bbox_projected")
        if proj_bbox_dict:
            proj_bbox = BoundingBox.from_dict(proj_bbox_dict, 4326)
            metadata.bbox_projected = proj_bbox
        else:
            metadata.bbox_projected = None

        metadata.raster_metadata = data.get("raster_metadata")

        return metadata

    @classmethod
    def from_geoseries(cls, series: gpd.GeoSeries) -> "AssetMetadata":
        return cls.from_dict(series.to_dict())

        # metadata = AssetMetadata()
        # metadata.asset_id = series["asset_id"]
        # metadata.item_id = series["item_id"]
        # metadata.collection_id = series["collection_id"]
        # metadata.tile_id = series["tile_id"]
        # metadata.title = series["title"]

        # metadata.href = series["href"]
        # metadata.original_href = series["original_href"]

        # asset_path = series["asset_path"]
        # metadata.asset_path = Path(asset_path) if asset_path else None

        # metadata.asset_type = series["asset_type"]
        # metadata.file_size = series["file_size"]

        # media_type = series["media_type"]
        # metadata.media_type = cls.mime_to_media_type(media_type)

        # metadata.datetime = series["datetime"].to_pydatetime()
        # metadata.start_datetime = series["start_datetime"].to_pydatetime()
        # metadata.end_datetime = series["end_datetime"].to_pydatetime()

        # metadata.shape = series["shape"]
        # metadata.tags = series["tags"]

        # bbox_dict = series["bbox_lat_lon"]
        # if bbox_dict:
        #     bbox = BoundingBox.from_dict(bbox_dict)
        #     assert bbox.epsg in (4326, None)
        #     metadata.bbox_lat_lon = bbox
        # else:
        #     metadata.bbox_lat_lon = None

        # # This is the essential difference between from_dict and from_geoseries
        # # TODO: reduce duplication between from_dict and from_geoseries, but geometry is an issue.
        # metadata.geometry_lat_lon = series["geometry"]

        # proj_bbox_dict = series["bbox_projected"]
        # if proj_bbox_dict:
        #     proj_bbox = BoundingBox.from_dict(proj_bbox_dict, 4326)
        #     metadata.bbox_projected = proj_bbox
        # else:
        #     metadata.bbox_projected = None

        # metadata.raster_metadata = series["raster_metadata"]

        # return metadata

    @staticmethod
    def __get_str_from_dict(key: str, data: Dict[str, Any]) -> Optional[str]:
        value = data.get(key)
        # preserve None, convert everything else to string.
        # return str(value) if value is not None else None
        if value is None:
            return None
        return str(value)

    @staticmethod
    def __as_type_from_dict(key: str, to_type: callable, data: Dict[str, Any]) -> Any:
        value = data.get(key)
        # preserve None, convert everything else to string.
        # return to_type(value) if value is not None else None
        if value is None:
            return None
        return to_type(value)

    def __str__(self):
        return str(self.to_dict())
