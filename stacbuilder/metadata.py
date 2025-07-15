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

import datetime as dt
from dataclasses import dataclass
from pathlib import Path
from typing import Any, ClassVar, Dict, List, Optional

import dateutil.parser
import geopandas as gpd
import numpy as np
import pandas as pd
from pydantic import BaseModel, ConfigDict, field_validator
from pystac.media_type import MediaType
from shapely.geometry import Polygon, mapping

from stacbuilder.boundingbox import BoundingBox
from stacbuilder.projections import project_polygon


@dataclass
class BandMetadata:
    """
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


class AssetMetadata(BaseModel):
    """Intermediate metadata that models the properties we actually use."""

    model_config = ConfigDict(
        arbitrary_types_allowed=True,
        validate_assignment=True,
    )

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
    # - Metadata from the raster about the bands,
    #       for the EO and Raster STAC extensions (eo:bands and raster:bands)
    PROPS_FROM_HREFS: ClassVar[List[str]] = [
        "item_id",
        "asset_id",
        "collection_id",
        "asset_type",
        "datetime",
        "start_datetime",
        "end_datetime",
        "title",
        "tile_id",
    ]
    # The asset_id is the unique identifier for the asset.
    asset_id: str

    # href is the path to the asset. At least one of the following needs to be set:
    href: str = None
    original_href: str = None
    asset_path: Path = None

    # Which STAC item this asset belongs to.
    # When a STAC item bundles multiple assets then the InputPathParser in extract_href_info
    # must provide a value for item_id.
    # However, when each asset corresponds to one STAC item then asset_id and item_id will
    # be identical. So if we don't get an explicit value for item_id we assume it is the same as
    # asset_id.
    item_id: str = None

    # We use asset_type to find the corresponding asset definition config in the CollectionConfig
    asset_type: Optional[str] = None

    # Temporal information of the asset. Only datetime is required, but we can also have
    # start_datetime and end_datetime.
    datetime: dt.datetime
    start_datetime: Optional[dt.datetime] = None
    end_datetime: Optional[dt.datetime] = None

    # The bounding boxes and geometries, in latitude-longitude and also the projected version.
    # At least one of these must be set. If bbox_projected is not set, then proj_epsg must also be set.
    bbox_lat_lon: BoundingBox = None
    bbox_projected: BoundingBox = None
    geometry_lat_lon: Polygon = None
    geometry_proj: Polygon = None
    proj_epsg: int = None

    # Affine transform as the raster file states it, in case we need it for CRS conversions.
    transform: Optional[List[float]] = None

    # Raster shape in pixels
    shape: Optional[List[int]] = None

    # file size, corresponds to file:size from FileInfo STAC extension
    file_size: Optional[int] = None

    # Tags in the raster.
    tags: dict[str, str] = {}

    # The bands in the raster file.
    bands: List[BandMetadata] = []

    tile_id: Optional[str] = None
    media_type: Optional[MediaType] = None

    # platforms: Optional[List[str]] = None
    # instruments: Optional[List[str]] = None
    # missions: Optional[List[str]] = None

    @field_validator("geometry_lat_lon", "geometry_proj")
    @classmethod
    def validate_geometry(cls, v):
        """Validate that geometry fields are Polygon instances."""
        if v is not None and not isinstance(v, Polygon):
            raise ValueError(f"Geometry must be a Polygon instance, got {type(v)}")
        return v

    @field_validator("datetime", "start_datetime", "end_datetime")
    @classmethod
    def validate_datetime_fields(cls, v):
        """Validate and convert datetime fields on every assignment."""
        if v is None:
            return v
        return check_datetime(v)

    def model_post_init(self, context: Any):
        """
        Pydantic method to overwrite that is ran after init of a new AssetMetadata instance.
        Run some checks on the properties that are set and infer some properties"""
        if not any([self.asset_id, self.item_id]):
            raise ValueError("At least one of asset_id or item_id must be set.")
        if not self.asset_path:
            self.asset_path = Path(self.original_href)
        if not self.href:
            self.href = str(self.asset_path)
        if not self.original_href:
            self.original_href = str(self.href)

        if not self.item_id:
            self.item_id = self.asset_id

        self._ensure_geoms()

    def _ensure_geoms(self):
        """Ensure that the geometries and bounding boxes are set correctly based on the available properties."""
        if self.bbox_projected:  # base case when bbox_projected is set
            if not self.proj_epsg:
                self.proj_epsg = self.bbox_projected.epsg

            if not self.geometry_proj:
                self.geometry_proj = self.bbox_projected.as_polygon()

            if not self.geometry_lat_lon:
                self.geometry_lat_lon = project_polygon(
                    geometry=self.geometry_proj, from_crs=self.proj_epsg, to_crs=4326
                )

            if not self.bbox_lat_lon:
                self.bbox_lat_lon = BoundingBox.from_list(self.geometry_lat_lon.bounds, epsg=4326)
        elif not self.proj_epsg:
            raise ValueError("proj_epsg must be set if bbox_projected is not set.")
        elif self.bbox_lat_lon:  # base case when bbox_lat_lon is set
            if not self.geometry_lat_lon:
                self.geometry_lat_lon = self.bbox_lat_lon.as_polygon()

            if not self.geometry_proj:
                self.geometry_proj = project_polygon(
                    geometry=self.geometry_lat_lon, from_crs=4326, to_crs=self.proj_epsg
                )

            self.bbox_projected = BoundingBox.from_list(self.geometry_proj.bounds, epsg=self.proj_epsg)
        elif self.geometry_proj:
            self.bbox_projected = BoundingBox.from_list(self.geometry_proj.bounds, epsg=self.proj_epsg)
            self._ensure_geoms()
        elif self.geometry_lat_lon:
            self.bbox_lat_lon = BoundingBox.from_list(self.geometry_lat_lon.bounds, epsg=4326)
            self._ensure_geoms()
        else:
            raise ValueError(
                "At least one of bbox_lat_lon, bbox_projected, geometry_lat_lon, or geometry_proj must be set."
            )

    @property
    def bbox_as_list(self) -> Optional[List[float]]:
        if not self.bbox_lat_lon:
            return None
        return self.bbox_lat_lon.to_list()

    @property
    def proj_bbox_as_list(self) -> Optional[List[float]]:
        if not self.bbox_projected:
            return None
        return self.bbox_projected.to_list()

    @property
    def geometry_lat_lon_as_dict(self) -> Optional[Dict[str, Any]]:
        if not self.geometry_lat_lon:
            return None
        return mapping(self.geometry_lat_lon)

    @property
    def geometry_proj_as_dict(self) -> Optional[Dict[str, Any]]:
        if not self.bbox_projected:
            return None
        return mapping(self.geometry_proj)

    @property
    def proj_geometry_as_wkt(self) -> Optional[str]:
        if not self.bbox_projected:
            return None
        return self.bbox_projected.as_wkt()

    @property
    def proj_bbox_as_polygon(self) -> Optional[Polygon]:
        if not self.bbox_projected:
            return None
        return self.bbox_projected.as_polygon()

    @property
    def year(self) -> Optional[int]:
        if not self.datetime:
            return None
        return self.datetime.year

    @property
    def month(self) -> Optional[int]:
        if not self.datetime:
            return None
        return self.datetime.month

    @property
    def day(self) -> Optional[int]:
        if not self.datetime:
            return None
        return self.datetime.day

    def to_dict(self, include_internal=False) -> Dict[str, Any]:
        """Convert to a dictionary for troubleshooting (output) or for other processing.

        :param include_internal:
            Include internal information for debugging, defaults to False.
        :return: A dictionary that represents the same metadata.
        """
        data = {
            "asset_id": self.asset_id,
            "item_id": self.item_id,
            "tile_id": self.tile_id,
            "href": self.href,
            "original_href": self.original_href,
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
            "transform": self.transform,
            "geometry_lat_lon": self.geometry_lat_lon,
            "file_size": self.file_size,
            "bands": [band.to_dict() for band in self.bands],
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

    def update_from_dict(self, data: Dict[str, Any]) -> None:
        """Update the metadata from a dictionary."""
        for key, value in data.items():
            if key in self.PROPS_FROM_HREFS:
                setattr(self, key, value)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "AssetMetadata":
        metadata = AssetMetadata()
        metadata.asset_id = cls.__get_str_from_dict("asset_id", data)
        metadata.item_id = cls.__get_str_from_dict("item_id", data)
        metadata.tile_id = cls.__get_str_from_dict("tile_id", data)

        metadata.href = cls.__get_str_from_dict("href", data)
        metadata.original_href = cls.__get_str_from_dict("original_href", data)
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

        metadata.transform = data.get("transform")

        proj_bbox_dict = data.get("bbox_projected")
        if proj_bbox_dict:
            proj_bbox = BoundingBox.from_dict(proj_bbox_dict)
            metadata.bbox_projected = proj_bbox
        else:
            metadata.bbox_projected = None

        return metadata

    @classmethod
    def from_geoseries(cls, series: gpd.GeoSeries) -> "AssetMetadata":
        # Delegate the conversion to `from_dict` for consistency.
        return cls.from_dict(series.to_dict())

    @staticmethod
    def __get_str_from_dict(key: str, data: Dict[str, Any]) -> Optional[str]:
        value = data.get(key)
        # Preserve None, convert everything else to string.
        if value is None:
            return None
        return str(value)

    @staticmethod
    def __as_type_from_dict(key: str, to_type: callable, data: Dict[str, Any]) -> Any:
        value = data.get(key)
        # preserve None, convert everything else to string.
        if value is None:
            return None
        return to_type(value)

    def __str__(self):
        return str(self.to_dict())

    def __eq__(self, other: "AssetMetadata") -> bool:
        if other is self:
            return True

        return all(
            [
                self.asset_id == other.asset_id,
                self.item_id == other.item_id,
                self.href == other.href,
                self.original_href == other.original_href,
                self.asset_path == other.asset_path,
                self.asset_type == other.asset_type,
                self.datetime == other.datetime,
                self.start_datetime == other.start_datetime,
                self.end_datetime == other.end_datetime,
                self.bbox_lat_lon == other.bbox_lat_lon,
                self.bbox_projected == other.bbox_projected,
                self.proj_epsg == other.proj_epsg,
                self.geometry_lat_lon == other.geometry_lat_lon,
                self.transform == other.transform,
                self.shape == other.shape,
                self.file_size == other.file_size,
                self.tags == other.tags,
                self.tile_id == other.tile_id,
                self.media_type == other.media_type,
            ]
        )

    def __gt__(self, other) -> bool:
        # for sorting support
        if other is self:
            return True
        return self.asset_id > other.asset_id

    def __ge__(self, other) -> bool:
        # for sorting support
        if other is self:
            return True
        return self.asset_id >= other.asset_id

    def __lt__(self, other) -> bool:
        # for sorting support
        if other is self:
            return True
        return self.asset_id < other.asset_id

    def __le__(self, other) -> bool:
        # for sorting support
        if other is self:
            return True
        return self.asset_id <= other.asset_id

    def get_differences(self, other: "AssetMetadata") -> Dict[str, Any]:
        if other is self:
            return {}

        differences = {}
        self_dict = self.to_dict()
        other_dict = other.to_dict()
        for key in self_dict.keys():
            if self_dict[key] != other_dict[key]:
                differences[key] = (self_dict[key], other_dict[key])

        return differences


def check_datetime(value) -> dt.datetime:
    if isinstance(value, dt.datetime):
        if value.tzinfo is None:
            # If the datetime has no timezone, assume it is UTC.
            value = value.replace(tzinfo=dt.UTC)
        return value

    if isinstance(value, dt.date):
        return convert_date_to_datetime(value)

    if isinstance(value, str):
        # if not value.endswith("Z") and not "+" in value:
        #     converted_value = value + "Z"
        # else:
        #     converted_value = value
        converted_value = dateutil.parser.parse(value)
        if not isinstance(converted_value, dt.datetime):
            return convert_date_to_datetime(converted_value)
        else:
            return converted_value

    raise TypeError(f"Can not convert this time to datetime: type={type(value)}, {value=}")


def convert_date_to_datetime(value: dt.date) -> dt.datetime:
    return dt.datetime(value.year, value.month, value.day, 0, 0, 0, tzinfo=dt.UTC)
