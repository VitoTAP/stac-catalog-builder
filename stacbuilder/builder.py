"""The core module of the STAC catalog builder.

This contains the classes that generate the STAC catalogs, collections and items.
"""

import datetime as dt
import json
import logging
import shutil
import tempfile
from functools import partial
from itertools import islice
from pathlib import Path
from typing import Any, Callable, Dict, Hashable, Iterable, List, Optional, Protocol, Tuple, Union


import geopandas as gpd
import pandas as pd
import rasterio
import rio_stac.stac as rst
from openeo.util import normalize_crs
from shapely.geometry import shape
from pystac import Asset, CatalogType, Collection, Extent, Item, SpatialExtent, TemporalExtent
from pystac.errors import STACValidationError
from pystac.layout import TemplateLayoutStrategy


# TODO: add the GridExtension support again
# from pystac.extensions.grid import GridExtension
from pystac.extensions.item_assets import AssetDefinition, ItemAssetsExtension
from pystac.extensions.projection import ItemProjectionExtension


# TODO: add the GridExtension support again
# from pystac.extensions.projection import ProjectionExtension
from pystac.extensions.raster import RasterExtension
from stactools.core.io import ReadHrefModifier


from stacbuilder.boundingbox import BoundingBox
from stacbuilder.exceptions import InvalidOperation, InvalidConfiguration
from stacbuilder.pathparsers import (
    InputPathParser,
    InputPathParserFactory,
)
from stacbuilder.config import (
    AssetConfig,
    AlternateHrefConfig,
    CollectionConfig,
    FileCollectorConfig,
)
from stacbuilder.metadata import AssetMetadata, BandMetadata, RasterMetadata
from stacbuilder.projections import reproject_bounding_box
from stacbuilder.timezoneformat import TimezoneFormatConverter


_logger = logging.getLogger(__name__)


CLASSIFICATION_SCHEMA = "https://stac-extensions.github.io/classification/v1.0.0/schema.json"
ALTERNATE_ASSETS_SCHEMA = "https://stac-extensions.github.io/alternate-assets/v1.1.0/schema.json"


class CreateAssetUrlFromPath:
    def __init__(self, href_template: str, data_root: Path) -> None:
        self.url_template = href_template
        self.data_root = Path(data_root)

    def __call__(self, asset_path: Path) -> str:
        """This method must match the signature of ReadHrefModifier.
        ReadHrefModifier is a type alias for Callable[[str], str]
        """
        return self.get_url(asset_path)

    def get_url(self, asset_path: Path):
        rel_path: Path = asset_path.relative_to(self.data_root)
        return self.url_template.format(str(rel_path))


AssetMetadataToURL = Callable[[AssetMetadata], str]


class AlternateHrefGenerator:
    """Generates the alternate links for assets."""

    def __init__(self):
        self._callbacks: Dict[str, AssetMetadataToURL] = {}

    def register_callback(self, key, converter=AssetMetadataToURL):
        self._callbacks[key] = converter

    def has_alternate_key(self, key: str) -> bool:
        return key in self._callbacks

    def get_alternates(self, asset_metadata: AssetMetadata) -> Dict[str, Dict[str, Dict[str, str]]]:
        alternates = {}
        for key in self._callbacks:
            alternates[key] = {"href": self.get_alternate_href_for(key, asset_metadata)}

        return {"alternate": alternates}

    def get_alternate_href_for(self, key: str, asset_metadata: AssetMetadata) -> Dict[str, str]:
        if not self.has_alternate_key(key):
            return None
        return self._callbacks[key](asset_metadata)

    def add_MEP(self):
        self.register_callback("MEP", lambda asset_md: str(asset_md.asset_path))

    def add_basic_S3(self, s3_bucket: str, s3_root_path: Optional[str] = None):
        """Add a S3 with an S3 bucket and the asset's file path concatenated to that bucket.

        For example:
            /my/data/folder/some-collection/some-asset.tif
        becomes:
            s3://my-bucket/my/data/folder/some-collection/some-asset.tif

        If you need to translate the file path in a more sophisticated wat you have to write your
        own handler.

        For example when the root of the path needs to be replaced by something else
        for the S3 urls. You need write a callback for that:

            /my/data/folder/some-collection/some-asset.tif -> s3://my-bucket/different-data-root/some-asset.tif
        """
        s3_bucket = self.remove_leading_trailing_slash(s3_bucket)
        s3_root_path = self.remove_leading_trailing_slash(s3_root_path) if s3_root_path else None

        convert = partial(self.to_S3_url, s3_bucket=s3_bucket, s3_root_path=s3_root_path)
        self.register_callback("S3", convert)

    @classmethod
    def to_S3_url(cls, asset_md: AssetMetadata, s3_bucket: str, s3_root_path: str) -> str:
        path = cls.remove_leading_trailing_slash(str(asset_md.asset_path))
        if s3_root_path:
            s3_url = f"s3://{s3_bucket}/{s3_root_path}/{path}"
        else:
            s3_url = f"s3://{s3_bucket}/{path}"
        return s3_url

    @staticmethod
    def remove_leading_trailing_slash(path: str):
        if path.startswith("/"):
            result = path[1:]
        else:
            result = path

        if result.endswith("/"):
            result = result[:-1]

        return result

    @classmethod
    def from_config(cls, config: AlternateHrefConfig) -> "AlternateHrefGenerator":
        alt_link_gen = AlternateHrefGenerator()
        if not config:
            return alt_link_gen

        if config.add_MEP:
            alt_link_gen.add_MEP()

        if config.add_S3:
            if not config.s3_bucket:
                raise InvalidConfiguration(
                    "AlternateHrefConfig specifies S3 links need to be added but there is no value for s3_bucket"
                )
            alt_link_gen.add_basic_S3(s3_bucket=config.s3_bucket, s3_root_path=config.s3_root_path)

        return alt_link_gen


def get_item_from_rio_stac(tiff_path: Path, collection_id: str, collection_file: Path):
    """Creates a STAC item from a GeoTIFF file, using rio-stac.

    This is the equivalent of the command `rio stac`.
    """
    return rst.create_stac_item(
        source=str(tiff_path),
        collection=collection_id,
        collection_url=str(collection_file),
    )


class IDataCollector(Protocol):
    """Interface/Protocol for all DataCollector implementations."""

    def collect(self) -> None:
        """Collect the data and store it internally.

        Each implementation needs to add a method to access the collected data,
        because a specific method will be more clear that what we could add here.

        At the level of this class here we could only add a method that returns
        `Any`, and with a generic name that will be a bit too vague.
        """
        ...

    def has_collected(self) -> bool:
        """Has the collection been done/ does the collector contain any data."""
        ...

    def reset(self) -> None:
        """Empty the collected data."""
        ...


class FileCollector(IDataCollector):
    """Collects geotiff files that match a glob, in a directory."""

    def __init__(
        self, input_dir: Optional[Path] = None, glob: Optional[str] = "*", max_files: Optional[int] = -1
    ) -> None:
        #
        # Settings: these are just data, not components we delegate work to.
        #
        # These are public members, or should have a public property.
        self.input_dir: Path = input_dir
        self.glob: str = glob
        self.max_files: int = max_files
        self._set_missing_fields_to_defaults()

        # The result
        self._input_files: List[Path] = None

    def _set_missing_fields_to_defaults(self):
        if not self.input_dir:
            self.input_dir = None

        if not self.glob:
            self.glob = "*"

        if not self.max_files:
            self.max_files = -1

    @staticmethod
    def from_config(config: FileCollectorConfig) -> "FileCollector":
        """Use the configuration object to create a new FileCollector instance."""
        collector = FileCollector()
        collector.setup(config)
        return collector

    def setup(self, config: FileCollectorConfig):
        """Read the settings for this instance from the configuration object."""
        self.input_dir = config.input_dir
        self.glob = config.glob
        self.max_files = config.max_files
        self._set_missing_fields_to_defaults()
        self.reset()

    def collect(self):
        input_files = (f for f in self.input_dir.glob(self.glob) if f.is_file())

        if self.max_files > 0:
            input_files = islice(input_files, self.max_files)

        self._input_files = list(input_files)

    def has_collected(self) -> bool:
        return self._input_files is not None

    def reset(self):
        # print(f"resetting {self.__class__.__name__} instance: {self}")
        self._input_files = None

    @property
    def input_files(self) -> List[Path]:
        """Get the collected input files."""
        return self._input_files or []


class IMetadataCollector(IDataCollector):
    """Interface/Protocol for collector that gets Metadata objects from a source.

    You need still to implement the method `collect`
    """

    def __init__(self):
        self._metadata_list: List[AssetMetadata] = None

    def has_collected(self) -> bool:
        return self._metadata_list is not None

    def reset(self):
        self._metadata_list = None

    @property
    def metadata(self) -> List[AssetMetadata]:
        return self._metadata_list or []


class ISTACItemCollector(IDataCollector):
    """Interface/Protocol for collector that gets STAC Items from a source.

    You need still to implement the method `collect`
    """

    def __init__(self):
        self._metadata_list: List[AssetMetadata] = None

    def has_collected(self) -> bool:
        return self._metadata_list is not None

    def reset(self):
        self._metadata_list = None

    @property
    def stac_items(self) -> List[Item]:
        return self._metadata_list or []


class IMapMetadataToSTACItem(Protocol):
    """Interface for a mapping that converts intermediate Metadata objects STAC Items.

    TODO: name could be better
    TODO: Will we really have multiple implementations or not? If probability low, remove IMapMetadataToSTACItem
    """

    def map(self, metadata: AssetMetadata) -> Item:
        """Converts a Metadata objects to a STAC Items."""
        ...

    def map_all(self, metadata_source: Iterable[AssetMetadata]) -> Iterable[Item]:
        """Return generator the converts all metadata objects to STAC Items"""
        return (self.map(metadata) for metadata in metadata_source)


class MapMetadataToSTACItem(IMapMetadataToSTACItem):
    """Converts Metadata objects to STAC Items.

    TODO: class name could be better
    TODO: find better name for item_assets_configs, maybe asset_definition_configs.
    """

    def __init__(
        self,
        item_assets_configs: Dict[str, AssetConfig],
        alternate_href_generator: Optional[AlternateHrefGenerator] = None,
    ) -> None:
        super().__init__()

        # Settings: these are just data, not components we delegate work to.
        self._item_assets_configs: item_assets_configs = item_assets_configs
        self._alternate_href_generator: Optional[AlternateHrefGenerator] = alternate_href_generator

    @property
    def item_assets_configs(self) -> Dict[str, AssetConfig]:
        return self._item_assets_configs

    def create_alternate_links(self, metadata: AssetMetadata) -> Dict[str, Any]:
        """Create the alternate links.

        TODO: make this configurable so we can handle both MEP, S3 and anything else.
        """
        if not self._alternate_href_generator:
            return None

        if not metadata.asset_path:
            return None

        return self._alternate_href_generator.get_alternates(metadata)

    def map(self, metadata: AssetMetadata) -> Item:
        if metadata.asset_type not in self.item_assets_configs:
            error_msg = (
                "Found an unknown item type, not defined in collection configuration: "
                + f"{metadata.asset_type}, returning item=None, "
                + f"{metadata.href=}"
            )
            _logger.warning(error_msg)
            return None

        item = Item(
            href=metadata.href,
            id=metadata.item_id,
            geometry=metadata.geometry_as_dict,
            bbox=metadata.bbox_as_list,
            datetime=metadata.datetime,
            start_datetime=metadata.start_datetime,
            end_datetime=metadata.end_datetime,
            properties={
                "product_version": metadata.version,
                # "product_tile": metadata.tile,
            },
        )
        # TODO: support optional parts: store the tile ID if dataset uses that.
        #   We would need a way to customize extracting that tile ID for the specific dataset.

        description = self.item_assets_configs[metadata.asset_type].description
        item.common_metadata.description = description

        item.common_metadata.created = dt.datetime.utcnow()

        # TODO: support optional parts: these fields are recommended but they are also not always relevant or present.
        # item.common_metadata.mission = constants.MISSION
        # item.common_metadata.platform = constants.PLATFORM
        # item.common_metadata.instruments = constants.INSTRUMENTS

        item.add_asset(metadata.asset_type, self._create_asset(metadata, item))
        item_proj = ItemProjectionExtension.ext(item, add_if_missing=True)
        item_proj.epsg = metadata.proj_epsg
        item_proj.bbox = metadata.proj_bbox_as_list
        item_proj.geometry = metadata.proj_geometry_as_dict
        item_proj.transform = metadata.transform
        item_proj.shape = metadata.shape

        # TODO: support optional parts: grid extension is recommended if we are indeed on a grid, but
        #    that is not always the case.
        # grid = GridExtension.ext(item, add_if_missing=True)
        # grid.code = f"TILE-{metadata.tile}"

        # TODO: Adding the eo:bands to the item this way below breaks the validation. Find out why.
        # EOExtension.add_to(item)
        # item_eo = EOExtension.ext(item, add_if_missing=True)
        # item_eo.bands = []
        # asset_config: AssetConfig = self._assets_config_for(metadata.asset_type)
        # eo_bands = []
        # for band_cfg in asset_config.eo_bands:
        #     new_band: Band = Band.create(
        #         name = band_cfg.name,
        #         description=band_cfg.description,
        #     )
        #     eo_bands.append(new_band)
        # item_eo.apply(eo_bands)

        item.stac_extensions.append(CLASSIFICATION_SCHEMA)
        item.stac_extensions.append(ALTERNATE_ASSETS_SCHEMA)

        return item

    def _create_asset(self, metadata: AssetMetadata, item: Item) -> Asset:
        asset_defs = self._get_assets_definitions()
        asset_def: AssetDefinition = asset_defs[metadata.asset_type]
        asset_config = self._get_assets_config_for(metadata.asset_type)
        asset: Asset = asset_def.create_asset(metadata.href)
        asset.set_owner(item)

        # TODO: add info from RasterExtension
        # Add import at top:
        from pystac.extensions.raster import RasterBand

        asset_raster = RasterExtension.ext(asset, add_if_missing=True)
        raster_bands = []

        if metadata.raster_metadata:
            # TODO: HACK: making assumptions here that each band in the raster appears in the same order as in our config.
            #   Would be better if we could identify the band by name,
            #   but the raster metadata may not even have any band names.
            if not asset_config.raster_bands:
                # There is no information to fill in default values for raster:bands
                # Just fill in what we do have.
                for band_md in metadata.raster_metadata.bands:
                    new_band: RasterBand = RasterBand.create(
                        data_type=band_md.data_type,
                        nodata=band_md.nodata,
                        unit=band_md.units,
                    )
                    raster_bands.append(new_band)
            else:
                # The default values for raster:bands are available in the configuration
                for i, raster_bands_config in enumerate(asset_config.raster_bands):
                    band_md = metadata.raster_metadata.bands[i]

                    new_band: RasterBand = RasterBand.create(
                        # TODO: need to get this information via rasterio as much as possible.
                        data_type=band_md.data_type or raster_bands_config.data_type,
                        nodata=band_md.nodata or raster_bands_config.nodata,
                        unit=band_md.units or raster_bands_config.unit,
                    )
                    if raster_bands_config.sampling is not None:
                        new_band.sampling = raster_bands_config.sampling.__str__()

                    if raster_bands_config.offset is not None:
                        new_band.offset = raster_bands_config.offset
                    if raster_bands_config.scale is not None:
                        new_band.scale = raster_bands_config.scale

                    if raster_bands_config.spatial_resolution is not None:
                        new_band.spatial_resolution = raster_bands_config.spatial_resolution

                    # if not new_band.unit and raster_bands_config.unit is not None:
                    #     new_band.unit = raster_bands_config.unit

                    raster_bands.append(new_band)

        asset_raster.apply(raster_bands)

        # Add the alternate links for the Alternate-Asset extension
        # see: https://github.com/stac-extensions/alternate-assets
        if metadata.asset_path:
            alternate_links = self.create_alternate_links(metadata)
            if alternate_links:
                asset.extra_fields.update(alternate_links)

        return asset

        # asset = Asset(href=make_absolute_href(metadata.href))
        # asset.title = asset_def.title
        # asset.description = asset_def.description
        # asset.roles = asset_def.roles
        # asset.media_type = asset_def.media_type

        # # asset.roles = ASSET_PROPS[self.band]["roles"]
        # # asset.title = ASSET_PROPS[self.band]["title"]
        # # asset.description = ASSET_PROPS[self.band]["description"]

        # # TODO: set the MediaType to use in the Metadata constructor
        # # asset.media_type = self.collection_config.media_type

        # # extra_fields = {"eo:bands": ASSET_PROPS[self.band]["bands"]}
        # # asset.extra_fields = extra_fields

        # return asset

    def _get_assets_definitions(self) -> List[AssetDefinition]:
        """Create AssetDefinitions, according to the config in self.item_assets_configs"""
        asset_definitions = {}
        for band_name, asset_config in self.item_assets_configs.items():
            asset_def: AssetDefinition = asset_config.to_asset_definition()
            # TODO: check whether we do need to store the collection as the owner here.
            # asset_def.owner = self.collection
            asset_definitions[band_name] = asset_def

        return asset_definitions

    def _get_assets_config_for(self, asset_type: str) -> AssetConfig:
        """Create AssetDefinitions, according to the config in self.item_assets_configs"""
        if asset_type not in self.item_assets_configs:
            return None
        return self.item_assets_configs[asset_type]


# TODO: Probably better to eliminate RasterBBoxReader now.
#    Putting all rester reading code inMapGeoTiffToAssetMetadata seems better.
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

        if not proj_epsg:
            proj_epsg = 4326

        bbox_projected = BoundingBox.from_list(list(dataset.bounds), epsg=proj_epsg)

        if proj_epsg in [4326, "EPSG:4326", "epsg:4326"]:
            bbox_lat_lon = bbox_projected
        else:
            west, south, east, north = bbox_projected.to_list()
            bbox_list = reproject_bounding_box(west, south, east, north, from_crs=dataset.crs, to_crs="epsg:4326")
            bbox_lat_lon = BoundingBox.from_list(bbox_list, epsg=4326)

        transform = list(dataset.transform)[0:6]

        return bbox_lat_lon, bbox_projected, transform


class MapGeoTiffToAssetMetadata:
    """Extracts AssetMetadata from each file.

    TODO: name could be better
    TODO: no usages detected anywhere => confirm this and remove it
    """

    def __init__(
        self,
        path_parser: InputPathParser,
        href_modifier: Optional[ReadHrefModifier] = None,
    ) -> None:
        # Store dependencies: components that have to be provided to constructor
        self._path_parser = path_parser
        # TODO: remove MakeRelativeToCollection as default. This is a hack to test it quickly.
        self._href_modifier = href_modifier or None

    def to_metadata(
        self,
        asset_path: Union[Path, str],
    ) -> AssetMetadata:
        if not asset_path:
            raise ValueError(
                f'Argument "asset_path" must have a value, it can not be None or the empty string. {asset_path=}'
            )
        if not isinstance(asset_path, (Path, str)):
            raise TypeError(f'Argument "asset_path" must be of type Path or str. {type(asset_path)=}, {asset_path=}')

        asset_meta = AssetMetadata(extract_href_info=self._path_parser)
        asset_meta.original_href = asset_path
        asset_meta.asset_path = asset_path
        asset_meta.asset_id = Path(asset_path).stem
        asset_meta.item_id = Path(asset_path).stem
        asset_meta.datetime = dt.datetime.utcnow()

        if self._href_modifier:
            asset_meta.href = self._href_modifier(asset_path)
        else:
            asset_meta.href = asset_path

        with rasterio.open(asset_path) as dataset:
            asset_meta.shape = dataset.shape
            bboxes_result = RasterBBoxReader.from_rasterio_dataset(dataset)
            asset_meta.bbox_lat_lon, asset_meta.bbox_projected, asset_meta.transform = bboxes_result

            bands = []
            tags = dataset.tags() or {}
            units = tags.get("units")
            for i in range(dataset.count):
                # TODO: if tags contains unit, add the unit
                band_md = BandMetadata(data_type=dataset.dtypes[i], index=i, nodata=dataset.nodatavals[i], units=units)
                bands.append(band_md)
            raster_meta = RasterMetadata(shape=dataset.shape, bands=bands)
            # TODO: Decide: do we really need the raster tags. If so, where is the best place to store them.
            asset_meta.tags = dataset.tags()
            # TODO: currently there are two places to store tags. That is confusing, pick one.
            asset_meta.raster_metadata = raster_meta

        asset_meta.process_href_info()
        return asset_meta

    # @staticmethod
    # def from_url(
    #     href: str,
    #     extract_href_info: InputPathParser,
    #     read_href_modifier: Optional[ReadHrefModifier] = None,
    # ) -> "AssetMetadata":
    #     meta = AssetMetadata(extract_href_info=extract_href_info, read_href_modifier=read_href_modifier)
    #     from urllib.parse import ParseResult, urlparse

    #     parsed_url: ParseResult = urlparse(href)
    #     if parsed_url.scheme and parsed_url.netloc:
    #         # It is indeed a URL, do we need to download it or can rasterio handle URLs?
    #         # Also the href modifier needs to do something different for local paths vs URLs.
    #         import warnings

    #         warnings.showwarning(
    #             "URL handling has not been implemented yet. Passing it directly to rasterio but it may not work"
    #         )
    #         meta._read_geotiff(Path(href))

    #     else:
    #         meta._read_geotiff(Path(href))

    #     return meta


class IGroupMetadataBy(Protocol):
    def group_by(self, iter_metadata) -> Dict[Hashable, List[AssetMetadata]]:
        ...


class GroupMetadataByYear(IGroupMetadataBy):
    def __init__(self) -> None:
        super().__init__()

    def group_by(self, iter_metadata) -> Dict[int, List[AssetMetadata]]:
        groups: Dict[int, AssetMetadata] = {}

        for metadata in iter_metadata:
            year = metadata.year
            if year not in groups:
                groups[year] = []
            groups[year].append(metadata)

        return groups


class GroupMetadataByAttribute(IGroupMetadataBy):
    def __init__(self, attribute_name: str) -> None:
        super().__init__()
        self._attribute_name = attribute_name

    def group_by(self, iter_metadata) -> Dict[Hashable, List[AssetMetadata]]:
        groups: Dict[int, AssetMetadata] = {}

        for metadata in iter_metadata:
            attr = getattr(metadata, self._attribute_name)
            value = attr() if callable(attr) else attr

            if value not in groups:
                groups[value] = []

            groups[value].append(metadata)

        return groups


class STACCollectionBuilder:
    """Creates a STAC Collection from STAC Items."""

    DEFAULT_EXTENT = Extent(
        SpatialExtent([-180.0, -90.0, 180.0, 90.0]),
        TemporalExtent(
            [
                [
                    dt.datetime.utcnow() - dt.timedelta(weeks=52),
                    dt.datetime.utcnow(),
                ]
            ]
        ),
    )

    def __init__(self, collection_config: CollectionConfig, output_dir: Path, overwrite: bool = False) -> None:
        # Settings: these are just data, not components we delegate work to.
        self._collection_config = collection_config
        self._output_dir = Path(output_dir)
        self._overwrite_output = overwrite

        # Internal temporary state
        self._stac_items: List[Item] = None

        # The result
        self._collection: Collection = None

    def reset(self):
        # print(f"resetting {self.__class__.__name__} instance: {self}")
        self._collection = None
        self._stac_items = None

    @property
    def output_dir(self) -> Path:
        """The directory where we should save "collection.json.

        (and the items as well, but those tend to be in subdirectories)
        """
        return self._output_dir

    @output_dir.setter
    def output_dir(self, directory: Path) -> None:
        self._output_dir = Path(directory)

    @property
    def overwrite_output(self) -> bool:
        return self._overwrite_output

    @overwrite_output.setter
    def overwrite_output(self, value: bool) -> None:
        self._overwrite_output = bool(value)

    @property
    def item_assets_configs(self) -> Dict[str, AssetConfig]:
        return self.collection_config.item_assets or {}

    @property
    def collection_file(self) -> Path:
        return self.output_dir / "collection.json"

    @property
    def collection(self) -> Optional[Collection]:
        return self._collection

    def build_collection(self, stac_items: Iterable[Item]) -> None:
        """Create and save the STAC collection."""
        self.reset()
        self._stac_items = list(stac_items) or []
        self.create_collection()
        self.save_collection()

        # We save before we validate, because when the validation fails we want
        # to be able to inspect the incorrect result.
        # self.validate_collection(self.collection)

    def create_collection(
        self,
    ):
        """Create a empty pystac.Collection for the dataset."""
        self._create_empty_collection()

        item: Item
        for item in self._stac_items:
            # for asset in item.assets:
            #     asset.owner = self._collection
            if item is None:
                continue
            self._collection.add_item(item)

        self._collection.update_extent_from_items()

        layout_template = self._collection_config.layout_strategy_item_template
        strategy = TemplateLayoutStrategy(item_template=layout_template)

        output_dir_str = str(self.output_dir)
        if output_dir_str.endswith("/"):
            output_dir_str = output_dir_str[-1]
        self._collection.normalize_hrefs(output_dir_str, strategy=strategy)

    def validate_collection(self, collection: Collection):
        """Run STAC validation on the collection."""
        try:
            num_items_validated = collection.validate_all(recursive=True)
        except STACValidationError as exc:
            print(exc)
            raise
        else:
            print(f"Collection valid: number of items validated: {num_items_validated}")

    def save_collection(self) -> None:
        """Save the STAC collection to file."""
        _logger.info("Saving files ...")

        if not self.output_dir.exists():
            self.output_dir.mkdir(parents=True)

        # NOTE: creating a self-contained collection allows to move the collection and item files
        # but this is not enough to also be able to move the assets.
        # The href links to asset files also have the be relative (to the location of the STAC item)
        # This needs to be done via the href_modifier
        self._collection.save(catalog_type=CatalogType.SELF_CONTAINED)

    @property
    def providers(self):
        return [p.to_provider() for p in self._collection_config.providers]

    def _create_empty_collection(self) -> None:
        """Creates a STAC Collection with no STAC items."""

        coll_config: CollectionConfig = self._collection_config
        collection = Collection(
            id=coll_config.collection_id,
            title=coll_config.title,
            description=coll_config.description,
            keywords=coll_config.keywords,
            providers=self.providers,
            extent=self.DEFAULT_EXTENT,
            # summaries=constants.SUMMARIES,
        )
        # TODO: Add support for summaries.

        item_assets_ext = ItemAssetsExtension.ext(collection, add_if_missing=True)
        item_assets_ext.item_assets = self._get_item_assets_definitions()

        RasterExtension.add_to(collection)
        collection.stac_extensions.append(CLASSIFICATION_SCHEMA)

        # TODO: Add support for links in the collection.
        ## collection.add_links(
        ##     [
        ##         constants.PRODUCT_FACT_SHEET,
        ##         constants.PROJECT_WEBSITE,
        ##     ]
        ## )

        self._collection = collection

    def _get_item_assets_definitions(self) -> List[AssetDefinition]:
        asset_definitions = {}
        asset_configs = self._collection_config.item_assets

        for band_name, asset_config in asset_configs.items():
            asset_def: AssetDefinition = asset_config.to_asset_definition()
            # TODO: check whether we do need to store the collection as the owner here.
            asset_def.owner = self.collection
            asset_definitions[band_name] = asset_def

        return asset_definitions


class PostProcessSTACCollectionFile:
    """Takes an existing STAC collection file and runs our common postprocessing steps

    Currently this include 2 steps:

    - Step 1) converting UTC timezones marked with TZ "Z" (AKA Zulu time)
        to the notation with "+00:00" . This will be removed when the related GitHub issue is fixed:

        See also https://github.com/Open-EO/openeo-geopyspark-driver/issues/568

    - Step 2) overriding specific key-value pairs in the collections's dictionary with fixed values that we want.
        This helps to set some values quickly when things don't quite work as expected.
        For example overriding proj:bbox in at the collection level.

        This is intended as a simple solution for situations where a quick result is needed and a bug fix may take too long.
    """

    def __init__(self, collection_overrides: Optional[Dict[str, Any]]) -> None:
        # Settings
        self._collection_overrides = collection_overrides or {}

    @property
    def collection_overrides(self) -> Optional[Dict[str, Any]]:
        return self._collection_overrides

    def process_collection(self, collection_file: Path, output_dir: Optional[Path] = None):
        out_dir: Path = output_dir or collection_file.parent
        new_coll_file, _ = self._create_post_proc_directory_structure(collection_file, out_dir)
        self._convert_timezones_encoded_as_z(collection_file, out_dir)

        if self.collection_overrides:
            self._override_collection_components(new_coll_file)

        # Check if the new file is still valid STAC.
        self._validate_collection(Collection.from_file(new_coll_file))

    @classmethod
    def _create_post_proc_directory_structure(cls, collection_file: Path, output_dir: Optional[Path] = None):
        in_place = False
        if not output_dir:
            in_place = True
        elif output_dir.exists() and output_dir.samefile(collection_file.parent):
            in_place = True

        # converted_out_dir: Path = output_dir or collection_file.parent
        item_paths = cls.get_item_paths_for_coll_file(collection_file)

        if in_place:
            converted_out_dir = collection_file.parent
            collection_converted_file = collection_file
            new_item_paths = item_paths
        else:
            converted_out_dir = output_dir
            collection_converted_file = output_dir / collection_file.name

            # Overwriting => remove and re-create the old directory
            if converted_out_dir.exists():
                shutil.rmtree(converted_out_dir)

            # (re)create the entire directory structure
            converted_out_dir.mkdir(parents=True)
            relative_paths = [ip.relative_to(collection_file.parent) for ip in item_paths]
            new_item_paths = [output_dir / rp for rp in relative_paths]

            sub_directories = set(p.parent for p in new_item_paths)
            for sub_dir in sub_directories:
                if not sub_dir.exists():
                    sub_dir.mkdir(parents=True)

        return collection_converted_file, new_item_paths

    @staticmethod
    def get_item_paths_for_collection(collection: Collection) -> List[Path]:
        items = collection.get_all_items()
        return [Path(item.self_href) for item in items]

    @classmethod
    def get_item_paths_for_coll_file(cls, collection_file: Path) -> List[Path]:
        collection = Collection.from_file(collection_file)
        return cls.get_item_paths_for_collection(collection)

    @classmethod
    def _convert_timezones_encoded_as_z(cls, collection_file: Path, output_dir: Path):
        print("Converting UTC timezones encoded as 'Z' to +00:00...")
        conv = TimezoneFormatConverter()
        out_dir = output_dir or collection_file.parent
        item_paths = cls.get_item_paths_for_coll_file(collection_file)
        conv.process_catalog(in_coll_path=collection_file, in_item_paths=item_paths, output_dir=out_dir)

    def _override_collection_components(self, collection_file: Path):
        print("Overriding components of STAC collection that we want to give some fixed value ...")
        data = self._load_collection_as_dict(collection_file)
        overrides = self.collection_overrides

        for key, new_value in overrides.items():
            key_path = key.split("/")
            deepest_key = key_path[-1]
            sub_dict = data

            for sub_key in key_path[:-1]:
                if sub_key not in sub_dict:
                    sub_dict[sub_key] = {}
                sub_dict = sub_dict[sub_key]
            sub_dict[deepest_key] = new_value

        self._save_collection_as_dict(data, collection_file)

    def _validate_collection(self, collection: Collection):
        """Run STAC validation on the collection."""
        try:
            num_items_validated = collection.validate_all(recursive=True)
        except STACValidationError as exc:
            print(exc)
            raise
        else:
            print(f"Collection valid: number of items validated: {num_items_validated}")

    @staticmethod
    def _load_collection_as_dict(coll_file: Path) -> dict:
        with open(coll_file, "r") as f_in:
            return json.load(f_in)

    @staticmethod
    def _save_collection_as_dict(data: Dict[str, Any], coll_file: Path) -> None:
        with open(coll_file, "w") as f_out:
            json.dump(data, f_out, indent=2)


class GeoTiffPipeline:
    """A pipeline to generate a STAC collection from a directory containing GeoTIFF files."""

    # TODO: split up for reuse: want 2 pipelines, 1 for geotiffs and 1 that converts OSCARS metadata

    def __init__(
        self,
        collection_config: CollectionConfig,
        file_collector: FileCollector,
        path_parser: InputPathParser,
        output_dir: Path,
        overwrite: Optional[bool] = False,
    ) -> None:
        # Settings: these are just data, not components we delegate work to.
        self._collection_config = collection_config
        self._output_base_dir: Path = GeoTiffPipeline._get_output_dir_or_default(output_dir)
        self._collection_dir: Path = None
        self._overwrite: bool = overwrite

        # Store dependencies: components that have to be provided to constructor
        self._file_collector = file_collector
        self._path_parser = path_parser

        # Components / dependencies that we set up internally
        self._geotiff_to_metadata_mapper: MapGeoTiffToAssetMetadata = None
        self._meta_to_stac_item_mapper: MapMetadataToSTACItem = None
        self._metadata_group_creator: IGroupMetadataBy = None

        self._collection_builder: STACCollectionBuilder = None

        # results
        self._collection: Optional[Collection] = None
        self._collection_groups: Dict[Hashable, Collection] = {}

    @property
    def collection(self) -> Collection | None:
        return self._collection

    @property
    def collection_file(self) -> Path | None:
        if not self.collection:
            return None

        return Path(self.collection.self_href)

    @property
    def collection_groups(self) -> Dict[Hashable, Collection] | None:
        return self._collection_groups

    @property
    def collection_config(self) -> CollectionConfig:
        return self._collection_config

    @property
    def item_assets_configs(self) -> Dict[str, AssetConfig]:
        return self._collection_config.item_assets or {}

    @property
    def collection_builder(self) -> STACCollectionBuilder:
        return self._collection_builder

    @property
    def file_collector(self) -> FileCollector:
        return self._file_collector

    @property
    def path_parser(self) -> InputPathParser:
        return self._path_parser

    @property
    def geotiff_to_metadata_mapper(self) -> MapGeoTiffToAssetMetadata:
        return self._geotiff_to_metadata_mapper

    @property
    def meta_to_stac_item_mapper(self) -> MapMetadataToSTACItem:
        return self._meta_to_stac_item_mapper

    @staticmethod
    def from_config(
        collection_config: CollectionConfig,
        file_coll_cfg: FileCollectorConfig,
        output_dir: Optional[Path] = None,
        overwrite: Optional[bool] = False,
    ) -> "GeoTiffPipeline":
        """Creates a GeoTiffPipeline from configurations.

        We want the two configuration objects to remain separate, because one is the
        general collection configuration which is typically read from a JSON file
        and the other defines what paths to read from and write too.
        Especially the options about output path can change a lot so these are
        specified via CLI options.

        For example they can be different for testing versus for final output,
        and each user would typically work in their own home or user data folders
        for test output.

        Also we do not want to mix these (volatile) path settings with the
        stable/fixed settings in the general config file.
        """
        if output_dir and not isinstance(output_dir, Path):
            raise TypeError(f"Argument output_dir (if not None) should be of type Path, {type(output_dir)=}")

        pipeline = GeoTiffPipeline(None, None, None, None)
        pipeline.setup(
            collection_config=collection_config,
            file_coll_cfg=file_coll_cfg,
            output_dir=output_dir.expanduser().absolute() if output_dir else None,
            overwrite=overwrite,
        )
        return pipeline

    def setup(
        self,
        collection_config: CollectionConfig,
        file_coll_cfg: FileCollectorConfig,
        output_dir: Optional[Path] = None,
        overwrite: Optional[bool] = False,
    ) -> None:
        # Settings: these are just data, not components we delegate work to.
        if collection_config is None:
            raise ValueError('Argument "input_path_parser" can not be None, must be a CollectionConfig instance.')

        if file_coll_cfg is None:
            raise ValueError('Argument "file_coll_cfg" can not be None, must be a FileCollectorConfig instance.')

        self._collection_config = collection_config
        self._output_base_dir = GeoTiffPipeline._get_output_dir_or_default(output_dir)
        self._overwrite = overwrite

        # Store dependencies: components that have to be provided to constructor
        self._file_collector = FileCollector.from_config(file_coll_cfg)

        if collection_config.input_path_parser:
            self._path_parser = InputPathParserFactory.from_config(collection_config.input_path_parser)
        else:
            self._path_parser = None

        self._setup_internals()

    @staticmethod
    def _get_output_dir_or_default(output_dir: Path | str | None) -> Path:
        return Path(output_dir) if output_dir else Path(tempfile.gettempdir())

    def _setup_internals(
        self,
        group: str | int | None = None,
    ) -> None:
        """Setup the internal components based on the components that we receive via dependency injection."""

        # TODO: implement href modified that translates file path to a URL with a configurable base URL
        href_modifier = None
        cfg_href_modifier = self._collection_config.asset_href_modifier
        if cfg_href_modifier:
            href_modifier = CreateAssetUrlFromPath(
                data_root=cfg_href_modifier.data_root, href_template=cfg_href_modifier.url_template
            )

        self._geotiff_to_metadata_mapper = MapGeoTiffToAssetMetadata(
            path_parser=self._path_parser, href_modifier=href_modifier
        )

        if not self._collection_config.alternate_links:
            alternate_href_generator = AlternateHrefGenerator()
            alternate_href_generator.add_MEP()
        else:
            alternate_href_generator = AlternateHrefGenerator.from_config(self._collection_config.alternate_links)
        self._meta_to_stac_item_mapper = MapMetadataToSTACItem(
            item_assets_configs=self.item_assets_configs,
            alternate_href_generator=alternate_href_generator,
        )
        self._metadata_group_creator = GroupMetadataByYear()

        if group and not self.has_grouping:
            raise InvalidOperation("You can only use collection groups when the pipeline is configured for grouping.")

        if group:
            self._collection_dir = self.get_collection_file_for_group(group)
        else:
            self._collection_dir = self._output_base_dir

        self._collection_builder = STACCollectionBuilder(
            collection_config=self._collection_config,
            overwrite=self._overwrite,
            output_dir=self._collection_dir,
        )

    def reset(self) -> None:
        # print(f"resetting {self.__class__.__name__} instance: {self}")
        self._collection = None
        self._collection_groups = {}
        self._file_collector.reset()

    def get_input_files(self) -> Iterable[Path]:
        """Collect the input files for processing."""
        if not self._file_collector.has_collected():
            self._file_collector.collect()

        for file in self._file_collector.input_files:
            yield file

    def get_metadata(self) -> Iterable[AssetMetadata]:
        """Generate the intermediate metadata objects, from the input files."""
        for file in self.get_input_files():
            yield self._geotiff_to_metadata_mapper.to_metadata(file)

    @property
    def has_grouping(self):
        return self._metadata_group_creator is not None

    def get_metadata_groups(self) -> Dict[Hashable, List[AssetMetadata]]:
        if not self.has_grouping:
            return None
        return self._metadata_group_creator.group_by(self.get_metadata())

    def get_item_groups(self) -> Dict[Hashable, List[Item]]:
        if not self.has_grouping:
            return None

        group_to_stac_items = {}
        for group, list_metadata in self.get_metadata_groups().items():
            list_items = list(self._meta_to_stac_item_mapper.map_all(list_metadata))
            group_to_stac_items[group] = list_items

        return group_to_stac_items

    def collect_stac_items(self):
        """Generate the intermediate STAC Item objects."""
        for file in self.get_input_files():
            metadata = self._geotiff_to_metadata_mapper.to_metadata(file)

            # TODO: implement grouping of several assets that belong to one item, here.

            stac_item = self._meta_to_stac_item_mapper.map(metadata)
            # Ignore the asset when the file was not a known asset type, for example it is
            # not a GeoTIFF or it is not one of the assets or bands we want to include.
            if stac_item:
                stac_item.validate()
                yield stac_item

    def get_metadata_as_geodataframe(self) -> gpd.GeoDataFrame:
        """Return a GeoDataFrame representing the intermediate metadata."""
        return GeodataframeExporter.metadata_to_geodataframe(list(self.get_metadata()))

    def get_metadata_as_dataframe(self) -> pd.DataFrame:
        """Return a pandas DataFrame representing the intermediate metadata, without the geometry."""
        return GeodataframeExporter.metadata_to_dataframe(list(self.get_metadata()))

    def get_stac_items_as_geodataframe(self) -> gpd.GeoDataFrame:
        """Return a GeoDataFrame representing the STAC Items."""
        return GeodataframeExporter.stac_items_to_geodataframe(list(self.collect_stac_items()))

    def get_stac_items_as_dataframe(self) -> pd.DataFrame:
        """Return a pandas DataFrame representing the STAC Items, without the geometry."""
        return GeodataframeExporter.stac_items_to_dataframe(list(self.collect_stac_items()))

    def build_collection(self):
        """Build the entire STAC collection."""
        self.reset()

        self._collection_builder.build_collection(self.collect_stac_items())
        self._collection = self._collection_builder.collection

        coll_file = self._collection_builder.collection_file
        post_processor = PostProcessSTACCollectionFile(collection_overrides=self._collection_config.overrides)
        post_processor.process_collection(coll_file)

    def get_collection_file_for_group(self, group: str | int):
        return self._output_base_dir / str(group)

    def build_grouped_collections(self):
        self.reset()

        if not self.has_grouping:
            raise InvalidOperation(f"This instance of {self.__class__.__name__} does not have grouping.")

        for group, metadata_list in sorted(self.get_item_groups().items()):
            self._setup_internals(group=group)

            self._collection_builder.build_collection(metadata_list)
            self._collection_groups[group] = self._collection_builder.collection

            coll_file = self._collection_builder.collection_file
            post_processor = PostProcessSTACCollectionFile(collection_overrides=self._collection_config.overrides)
            post_processor.process_collection(coll_file)


# class HRLVPPMetadataCollector(IMetadataCollector):
#     def __init__(self):
#         super().__init__()

#     def collect(self):
#         pass


#     def convert_to_stac_items(df: pd.DataFrame):
#         for i in range(len(df)):
#             record = df.iloc[i, :]
#             metadata = AssetMetadata.from_geoseries(record)
#             pprint(metadata.to_dict())


class AssetMetadataPipeline:
    """Converts AssetMetadata to STAC collections."""

    def __init__(
        self,
        metadata_collector: IMetadataCollector,
        output_dir: Path,
        overwrite: Optional[bool] = False,
    ) -> None:
        # Components / dependencies that must be provided
        self._metadata_collector: IMetadataCollector = metadata_collector

        # Settings: these are just data, not components we delegate work to.
        self._output_base_dir: Path = self._get_output_dir_or_default(output_dir)
        self._collection_dir: Path = None
        self._overwrite: bool = overwrite

        # Components / dependencies that we set up internally
        self._geotiff_to_metadata_mapper: MapGeoTiffToAssetMetadata = None
        self._meta_to_stac_item_mapper: MapMetadataToSTACItem = None
        self._metadata_group_creator: IGroupMetadataBy = None

        self._collection_builder: STACCollectionBuilder = None

        # results
        self._collection: Optional[Collection] = None
        self._collection_groups: Dict[Hashable, Collection] = {}

    @property
    def collection(self) -> Collection | None:
        return self._collection

    @property
    def collection_file(self) -> Path | None:
        if not self.collection:
            return None

        return Path(self.collection.self_href)

    @property
    def collection_groups(self) -> Dict[Hashable, Collection] | None:
        return self._collection_groups

    @property
    def collection_config(self) -> CollectionConfig:
        return self._collection_config

    @property
    def item_assets_configs(self) -> Dict[str, AssetConfig]:
        return self._collection_config.item_assets or {}

    @property
    def collection_builder(self) -> STACCollectionBuilder:
        return self._collection_builder

    @property
    def file_collector(self) -> FileCollector:
        return self._file_collector

    @property
    def path_parser(self) -> InputPathParser:
        return self._path_parser

    @property
    def geotiff_to_metadata_mapper(self) -> MapGeoTiffToAssetMetadata:
        return self._geotiff_to_metadata_mapper

    @property
    def meta_to_stac_item_mapper(self) -> MapMetadataToSTACItem:
        return self._meta_to_stac_item_mapper

    @staticmethod
    def from_config(
        metadata_collector: IMetadataCollector,
        collection_config: CollectionConfig,
        output_dir: Optional[Path] = None,
        overwrite: Optional[bool] = False,
    ) -> "AssetMetadataPipeline":
        """Creates a GeoTiffPipeline from configurations.

        We want the two configuration objects to remain separate, because one is the
        general collection configuration which is typically read from a JSON file
        and the other defines what paths to read from and write too.
        Especially the options about output path can change a lot so these are
        specified via CLI options.

        For example they can be different for testing versus for final output,
        and each user would typically work in their own home or user data folders
        for test output.

        Also we do not want to mix these (volatile) path settings with the
        stable/fixed settings in the general config file.
        """
        pipeline = AssetMetadataPipeline(
            metadata_collector=metadata_collector, collection_config=None, output_dir=None, overwrite=False
        )
        pipeline.setup(
            collection_config=collection_config,
            output_dir=output_dir,
            overwrite=overwrite,
        )
        return pipeline

    def setup(
        self,
        collection_config: CollectionConfig,
        output_dir: Optional[Path] = None,
        overwrite: Optional[bool] = False,
    ) -> None:
        # Settings: these are just data, not components we delegate work to.
        if collection_config is None:
            raise ValueError('Argument "input_path_parser" can not be None, must be a CollectionConfig instance.')

        self._collection_config = collection_config
        self._output_base_dir = self._get_output_dir_or_default(output_dir)
        self._overwrite = overwrite

        self._setup_internals()

    @staticmethod
    def _get_output_dir_or_default(output_dir: Path | str | None) -> Path:
        return Path(output_dir) if output_dir else Path(tempfile.gettempdir())

    def _setup_internals(
        self,
        # group: str | int | None = None,
    ) -> None:
        """Setup the internal components based on the components that we receive via dependency injection."""

        # TODO: implement href modified that translates file path to a URL with a configurable base URL
        href_modifier = None
        cfg_href_modifier = self._collection_config.asset_href_modifier
        if cfg_href_modifier:
            href_modifier = CreateAssetUrlFromPath(
                data_root=cfg_href_modifier.data_root, href_template=cfg_href_modifier.url_template
            )

        self._geotiff_to_metadata_mapper = MapGeoTiffToAssetMetadata(
            path_parser=self._path_parser, href_modifier=href_modifier
        )
        self._meta_to_stac_item_mapper = MapMetadataToSTACItem(item_assets_configs=self.item_assets_configs)
        self._metadata_group_creator = GroupMetadataByYear()

        # if group and not self.has_grouping:
        #     raise InvalidOperation("You can only use collection groups when the pipeline is configured for grouping.")

        # if group:
        #     self._collection_dir = self.get_collection_file_for_group(group)
        # else:
        #     self._collection_dir = self._output_base_dir

        self._collection_builder = STACCollectionBuilder(
            collection_config=self._collection_config,
            overwrite=self._overwrite,
            output_dir=self._collection_dir,
        )

    def reset(self) -> None:
        # print(f"resetting {self.__class__.__name__} instance: {self}")
        self._collection = None
        self._collection_groups = {}

    # def get_input_files(self) -> Iterable[Path]:
    #     """Collect the input files for processing."""
    #     if not self._file_collector.has_collected():
    #         self._file_collector.collect()

    #     for file in self._file_collector.input_files:
    #         yield file

    def get_metadata(self) -> Iterable[AssetMetadata]:
        """Generate the intermediate metadata objects, from the input files."""
        for file in self.get_input_files():
            yield self._geotiff_to_metadata_mapper.to_metadata(file)

    # @property
    # def has_grouping(self):
    #     return self._metadata_group_creator is not None

    # def get_metadata_groups(self) -> Dict[Hashable, List[AssetMetadata]]:
    #     if not self.has_grouping:
    #         return None
    #     return self._metadata_group_creator.group_by(self.get_metadata())

    # def get_item_groups(self) -> Dict[Hashable, List[Item]]:
    #     if not self.has_grouping:
    #         return None

    #     group_to_stac_items = {}
    #     for group, list_metadata in self.get_metadata_groups().items():
    #         list_items = list(self._meta_to_stac_item_mapper.map_all(list_metadata))
    #         group_to_stac_items[group] = list_items

    #     return group_to_stac_items

    def collect_stac_items(self):
        """Generate the intermediate STAC Item objects."""
        for file in self.get_input_files():
            metadata = self._geotiff_to_metadata_mapper.to_metadata(file)

            # TODO: implement grouping of several assets that belong to one item, here.

            stac_item = self._meta_to_stac_item_mapper.map(metadata)
            # Ignore the asset when the file was not a known asset type, for example it is
            # not a GeoTIFF or it is not one of the assets or bands we want to include.
            if stac_item:
                stac_item.validate()
                yield stac_item

    def get_metadata_as_geodataframe(self) -> gpd.GeoDataFrame:
        """Return a GeoDataFrame representing the intermediate metadata."""
        return GeodataframeExporter.metadata_to_geodataframe(list(self.get_metadata()))

    def get_metadata_as_dataframe(self) -> pd.DataFrame:
        """Return a pandas DataFrame representing the intermediate metadata, without the geometry."""
        return GeodataframeExporter.metadata_to_dataframe(list(self.get_metadata()))

    def get_stac_items_as_geodataframe(self) -> gpd.GeoDataFrame:
        """Return a GeoDataFrame representing the STAC Items."""
        return GeodataframeExporter.stac_items_to_geodataframe(list(self.collect_stac_items()))

    def get_stac_items_as_dataframe(self) -> pd.DataFrame:
        """Return a pandas DataFrame representing the STAC Items, without the geometry."""
        return GeodataframeExporter.stac_items_to_dataframe(list(self.collect_stac_items()))

    def build_collection(self):
        """Build the entire STAC collection."""
        self.reset()

        self._collection_builder.build_collection(self.collect_stac_items())
        self._collection = self._collection_builder.collection

        coll_file = self._collection_builder.collection_file
        post_processor = PostProcessSTACCollectionFile(collection_overrides=self._collection_config.overrides)
        post_processor.process_collection(coll_file)

    # def get_collection_file_for_group(self, group: str | int):
    #     return self._output_base_dir / str(group)

    # def build_grouped_collections(self):
    #     self.reset()

    #     if not self.has_grouping:
    #         raise InvalidOperation(f"This instance of {self.__class__.__name__} does not have grouping.")

    #     for group, metadata_list in sorted(self.get_item_groups().items()):
    #         self._setup_internals(group=group)

    #         self._collection_builder.build_collection(metadata_list)
    #         self._collection_groups[group] = self._collection_builder.collection

    #         coll_file = self._collection_builder.collection_file
    #         post_processor = PostProcessSTACCollectionFile(collection_overrides=self._collection_config.overrides)
    #         post_processor.process_collection(coll_file)


class GeodataframeExporter:
    """Utitlity class to export metadata and STAC items as geopandas GeoDataframes.

    TODO: find a better name for GeodataframeExporter
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
