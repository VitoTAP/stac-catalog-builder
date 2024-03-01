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
import json
from pathlib import Path
from types import NoneType
from typing import Any, Dict, List, Optional, Tuple, Union


import dateutil.parser
import geopandas as gpd
import numpy as np
import pandas as pd
from pystac import Collection, Item
from shapely.geometry import Polygon, shape
from pystac.media_type import MediaType

from stacbuilder.boundingbox import BoundingBox
from stacbuilder.exceptions import InvalidOperation
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


class AssetMetadata:
    """Intermediate metadata that models the properties we actually use."""

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

    PROPS_FROM_HREFS = [
        "item_id",
        "asset_id",
        "collection_id",
        "asset_type",
        "datetime",
        "start_datetime",
        "end_datetime",
        "title",
        "tile_id",
        "item_href",
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

        #
        # Essential asset info: what asset is it and what STAC item does it belong to
        # What bands does it contain or correspond to.
        #

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

        # We use asset_type to find the corresponding asset definition config in the CollectionConfig
        self._asset_type: Optional[str] = None

        # Temporal extent
        # everything to do with the date + time and temporal extent this asset corresponds to.
        self._datetime = None
        self._start_datetime: Optional[dt.datetime] = None
        self._end_datetime: Optional[dt.datetime] = None

        # The bounding boxes, in latitude-longitude and also the projected version.
        self._bbox_lat_lon: Optional[BoundingBox] = None
        self._bbox_projected: Optional[BoundingBox] = None
        self._proj_epsg: Optional[int] = None

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
        """Fills in metadata fields with values extracted from the asset's file path or href.
        We receive the values as a dictionary, from the InputPathParser.

        TODO: Generalize process_href_info, so the data can come from any source, not only from hrefs/paths.
            Rationale:
            - Essentially process_href_info only fills in a known set of fields, copying data from
                a dict that we extracted from any source we want.
            - Where we get the dictionary does not matter, as long as we can provide that information somehow.
            - This set of fields are things we that AssetMetadata can not automatically derive.
                They have to be received from the source data, but we support more than one source.
                Currently we have two sources: OpenSearch and GeoTIFF files.
                Likely, netCDF will  next.
            - We don't have to keep this method, just the principle that we get a set of key-value pairs
                from something that processes source data, and have a more standardized mechanism
                to update the AssetMetadata object with that data.
        """
        href_info = self._extract_href_info.parse(self.href)
        self._info_from_href = href_info
        for key, value in href_info.items():
            # Ignore keys that do not match any attribute that should come from the href.
            if key in self.PROPS_FROM_HREFS:
                setattr(self, key, value)

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
        if bbox and bbox.epsg:
            self._proj_epsg = bbox.epsg

    @property
    def proj_bbox_as_list(self) -> Optional[List[float]]:
        # TODO: [decide] convert this RO property to a method or not?
        if not self._bbox_projected:
            return None
        return self._bbox_projected.to_list()

    @property
    def proj_epsg(self) -> Optional[int]:
        return self._proj_epsg

    @proj_epsg.setter
    def proj_epsg(self, value: Optional[int]) -> None:
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
            "transform": self.transform,
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

        metadata.transform = data.get("transform")

        metadata.geometry_lat_lon = data.get("geometry_lat_lon") or data.get("geometry")

        proj_bbox_dict = data.get("bbox_projected")
        if proj_bbox_dict:
            proj_bbox = BoundingBox.from_dict(proj_bbox_dict)
            metadata.bbox_projected = proj_bbox
        else:
            metadata.bbox_projected = None

        metadata.raster_metadata = data.get("raster_metadata")

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
                self._asset_id == other._asset_id,
                self._item_id == other._item_id,
                self._href == other._href,
                self._original_href == other._original_href,
                self._asset_path == other._asset_path,
                self._asset_type == other._asset_type,
                self._datetime == other._datetime,
                self._start_datetime == other._start_datetime,
                self._end_datetime == other._end_datetime,
                self._bbox_lat_lon == other._bbox_lat_lon,
                self._bbox_projected == other._bbox_projected,
                self._proj_epsg == other._proj_epsg,
                self._geometry_lat_lon == other._geometry_lat_lon,
                self.transform == other.transform,
                self.shape == other.shape,
                self.file_size == other.file_size,
                self.tags == other.tags,
                self.raster_metadata == other.raster_metadata,
                self.title == other.title,
                self.collection_id == other.collection_id,
                self.tile_id == other.tile_id,
                self.item_href == other.item_href,
                self.media_type == other.media_type,
            ]
        )

    def __gt__(self, other) -> bool:
        # for sorting support
        if other is self:
            return True
        return self._asset_id > other._asset_id

    def __ge__(self, other) -> bool:
        # for sorting support
        if other is self:
            return True
        return self._asset_id >= other._asset_id

    def __lt__(self, other) -> bool:
        # for sorting support
        if other is self:
            return True
        return self._asset_id < other._asset_id

    def __le__(self, other) -> bool:
        # for sorting support
        if other is self:
            return True
        return self._asset_id <= other._asset_id

    def get_differences(self, other) -> Dict[str, Any]:
        if other is self:
            return {}

        differences = {}
        self_dict = self.to_dict()
        other_dict = other.to_dict()
        for key in self_dict.keys():
            if self_dict[key] != other_dict[key]:
                differences[key] = (self_dict[key], other_dict[key])

        return differences


class GeodataframeExporter:
    """Utility class to export metadata and STAC items as geopandas GeoDataframes.

    TODO: This is currently a class with only static methods, perhaps a module would be beter.
    """

    @classmethod
    def stac_items_to_geodataframe(cls, stac_item_list: List[Item]) -> gpd.GeoDataFrame:
        if not stac_item_list:
            raise InvalidOperation("stac_item_list is empty or None. Can not create a GeoDataFrame")

        if not isinstance(stac_item_list, list):
            stac_item_list = list(stac_item_list)

        epsg = stac_item_list[0].properties.get("proj:epsg", 4326)
        records = cls.convert_dict_records_to_strings(i.to_dict() for i in stac_item_list)
        # TODO: limit the number of fields to what we need to see. Something like the code below.
        #
        # Not working yet, coming back to this but it is not a priority.
        #
        # it: Item
        # records = cls.convert_records_to_strings(
        #     (it.id, it.collection_id, it.bbox, it.datetime) for it in stac_item_list
        # )
        shapes = [shape(item.geometry) for item in stac_item_list]
        return gpd.GeoDataFrame(records, crs=epsg, geometry=shapes)

    @staticmethod
    def stac_items_to_dataframe(stac_item_list: List[Item]) -> pd.DataFrame:
        """Return a pandas DataFrame representing the STAC Items, without the geometry."""
        return pd.DataFrame.from_records(md.to_dict() for md in stac_item_list)

    @classmethod
    def metadata_to_geodataframe(cls, metadata_list: List[AssetMetadata]) -> gpd.GeoDataFrame:
        """Return a GeoDataFrame representing the intermediate metadata."""
        if not metadata_list:
            raise InvalidOperation("Metadata_list is empty or None. Can not create a GeoDataFrame")

        epsg = metadata_list[0].proj_epsg
        geoms = [m.proj_bbox_as_polygon for m in metadata_list]
        records = cls.convert_dict_records_to_strings(m.to_dict() for m in metadata_list)

        return gpd.GeoDataFrame(records, crs=epsg, geometry=geoms)

    @staticmethod
    def metadata_to_dataframe(metadata_list: List[AssetMetadata]) -> pd.DataFrame:
        """Return a pandas DataFrame representing the intermediate metadata, without the geometry."""
        if not metadata_list:
            raise InvalidOperation("Metadata_list is empty or None. Can not create a GeoDataFrame")

        return pd.DataFrame.from_records(md.to_dict() for md in metadata_list)

    @staticmethod
    def convert_dict_records_to_strings(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        out_records = [dict(rec) for rec in records]
        for rec in out_records:
            for key, val in rec.items():
                if isinstance(val, dt.datetime):
                    rec[key] = val.isoformat()
                elif isinstance(val, list):
                    rec[key] = json.dumps(val)
                elif isinstance(val, (int, float, bool, str)):
                    rec[key] = val
                else:
                    rec[key] = str(val)
        return out_records

    @staticmethod
    def convert_records_to_strings(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        temp_records = list(records)
        out_records = []

        for record in temp_records:
            convert_record = []
            for column in record:
                if isinstance(column, dt.datetime):
                    convert_record.append(column.isoformat())
                elif isinstance(column, list):
                    convert_record.append(json.dumps(column))
                elif isinstance(column, (int, float, bool, str)):
                    convert_record.append(column)
                else:
                    convert_record.append(str(column))
            out_records.append(convert_record)
        return out_records

    @staticmethod
    def save_geodataframe(gdf: gpd.GeoDataFrame, out_dir: Path, table_name: str) -> None:
        shp_dir = out_dir / "shp"
        if not shp_dir.exists():
            shp_dir.mkdir(parents=True)

        csv_path = out_dir / f"{table_name}.pipe.csv"
        shapefile_path = out_dir / f"shp/{table_name}.shp"
        parquet_path = out_dir / f"{table_name}.parquet"

        print(f"Saving pipe-separated CSV file to: {csv_path}")
        gdf.to_csv(csv_path, sep="|")

        # TODO: Shapefile has too many problems with unsupported column types. Going to remove it (but in a separate branch/PR).
        print(f"Saving shapefile to: {shapefile_path }")
        gdf.to_file(shapefile_path)

        print(f"Saving geoparquet to: {parquet_path}")
        gdf.to_parquet(parquet_path)

    @staticmethod
    def visualization_dir(collection: Collection):
        collection_path = Path(collection.self_href)
        return collection_path.parent / "tmp" / "visualization" / collection.id

    @classmethod
    def export_item_bboxes(cls, collection: Collection):
        out_dir: Path = cls.visualization_dir(collection)
        if not out_dir.exists():
            out_dir.mkdir(parents=True)

        items = collection.get_all_items()
        gdf: gpd.GeoDataFrame = cls.stac_items_to_geodataframe(items)
        cls.save_geodataframe(gdf, out_dir, "stac_item_bboxes")
