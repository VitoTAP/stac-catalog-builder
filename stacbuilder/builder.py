"""The core module of the STAC catalog builder.

This contains the classes that generate the STAC catalogs, collections and items.
"""

import datetime as dt
import json
import logging
import pprint
import shutil
import tempfile
from itertools import islice
from pathlib import Path
from typing import Any, Dict, Hashable, Iterable, List, Optional, Protocol


import geopandas as gpd
import pandas as pd
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

import rio_stac.stac as rst


from stacbuilder.exceptions import InvalidOperation
from stacbuilder.pathparsers import (
    InputPathParser,
    InputPathParserFactory,
)
from stacbuilder.config import AssetConfig, CollectionConfig, FileCollectorConfig
from stacbuilder.metadata import AssetMetadata
from stacbuilder.timezoneformat import TimezoneFormatConverter


_logger = logging.getLogger(__name__)


CLASSIFICATION_SCHEMA = "https://stac-extensions.github.io/classification/v1.0.0/schema.json"


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
        print(f"resetting {self.__class__.__name__} instance: {self}")
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

    def __init__(self, item_assets_configs: Dict[str, AssetConfig]) -> None:
        super().__init__()

        # Settings: these are just data, not components we delegate work to.
        self._item_assets_configs: item_assets_configs = item_assets_configs

    @property
    def item_assets_configs(self) -> Dict[str, AssetConfig]:
        return self._item_assets_configs

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

        item.add_asset(metadata.asset_type, self._create_asset(metadata))

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

        RasterExtension.add_to(item)
        item.stac_extensions.append(CLASSIFICATION_SCHEMA)

        return item

    def _create_asset(self, metadata: AssetMetadata) -> Asset:
        asset_defs = self._get_assets_definitions()
        asset_def: AssetDefinition = asset_defs[metadata.asset_type]
        return asset_def.create_asset(metadata.href)

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


class MapGeoTiffToSTACItem:
    """Extracts STAC Items from each file.

    TODO: name could be better
    """

    def __init__(self, path_parser: InputPathParser, map_metadata_to_stac_item: IMapMetadataToSTACItem) -> None:
        # Store dependencies: components that have to be provided to constructor
        self._path_parser = path_parser
        self._metadata_to_stac_item = map_metadata_to_stac_item

    def to_stac_item(self, file: Path) -> Item:
        """Generate the STAC Item for the specified GeoTIFF path."""
        metadata = self.to_metadata(file)
        return self._metadata_to_stac_item.map(metadata)

    def to_metadata(self, file: Path) -> AssetMetadata:
        """Generate the intermediate Metadata for the specified GeoTIFF path."""
        return AssetMetadata.from_href(
            href=str(file),
            extract_href_info=self._path_parser,
            read_href_modifier=None,
        )

    def map_all(self, files: Iterable[Path]) -> Iterable[AssetMetadata]:
        """Return generator the converts all files to STAC Items"""
        return (self.to_stac_item(file) for file in files)


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
        print(f"resetting {self.__class__.__name__} instance: {self}")
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
            assert item is not None
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
        print("Converting UeTC timezones encoded as 'Z' to +00:00...")
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
        # TODO: eliminate self.output_dir and self.overwrite
        self._output_base_dir: Path = GeoTiffPipeline._get_output_dir_or_default(output_dir)
        self._collection_dir: Path = None
        self._overwrite: bool = overwrite

        # Store dependencies: components that have to be provided to constructor
        self._file_collector = file_collector
        self._path_parser = path_parser

        # Components / dependencies that we set up internally
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
    def meta_to_stac_item_mapper(self) -> MapGeoTiffToSTACItem:
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
        pipeline = GeoTiffPipeline(None, None, None, None)
        pipeline.setup(
            collection_config=collection_config,
            file_coll_cfg=file_coll_cfg,
            output_dir=output_dir,
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

    def _setup_internals(self, group: str | int | None = None) -> None:
        """Setup the internal components based on the components that we receive via dependency injection."""
        # if self._collection_builder:
        #     # It has already been set up => skip it.
        #     # TODO: eliminate this issue or log a warning.
        #     # TODO: fix logging, currently no longer works.
        #     return

        self._meta_to_stac_item_mapper = MapMetadataToSTACItem(item_assets_configs=self.item_assets_configs)
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
        print(f"resetting {self.__class__.__name__} instance: {self}")
        self._collection = None
        self._collection_groups = {}
        # self._collection_builder.reset()
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
            yield AssetMetadata.from_href(
                href=str(file),
                extract_href_info=self._path_parser,
                read_href_modifier=None,
            )

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
            metadata = AssetMetadata.from_href(
                href=str(file),
                extract_href_info=self._path_parser,
                read_href_modifier=None,
            )
            stac_item = self._meta_to_stac_item_mapper.map(metadata)
            # Ignore it when the file was not a known type, for example it is
            # not a GeoTIFF or it is not one of the assets or bands we want to include.
            if stac_item:
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


def convert_fields_to_string(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out_records = [dict(rec) for rec in records]
    for rec in out_records:
        for key, val in rec.items():
            if isinstance(val, dt.datetime):
                rec[key] = val.isoformat()
            elif isinstance(val, list):
                rec[key] = json.dumps(val)
    return out_records


def _visualization_dir(collection: Collection):
    collection_path = Path(collection.self_href)
    return collection_path.parent / "tmp" / "visualization" / collection.id


class GeodataframeExporter:
    """Utitlity class to export metadata and STAC items as geopandas GeoDataframes.

    TODO: find a better name for GeodataframeExporter
    TODO: This is currently a class with only static methods, perhaps a module would be beter.
    """

    @staticmethod
    def stac_items_to_geodataframe(stac_item_list: List[Item]) -> gpd.GeoDataFrame:
        if not stac_item_list:
            raise InvalidOperation("stac_item_list is empty or None. Can not create a GeoDataFrame")

        epsg = stac_item_list[0].properties.get("proj:epsg", 4326)
        records = convert_fields_to_string(i.to_dict() for i in stac_item_list)
        shapes = [shape(item.geometry) for item in stac_item_list]
        return gpd.GeoDataFrame(records, crs=epsg, geometry=shapes)

    @staticmethod
    def stac_items_to_dataframe(stac_item_list: List[Item]) -> pd.DataFrame:
        """Return a pandas DataFrame representing the STAC Items, without the geometry."""
        return pd.DataFrame.from_records(md.to_dict() for md in stac_item_list)

    @staticmethod
    def metadata_to_geodataframe(metadata_list: List[AssetMetadata]) -> gpd.GeoDataFrame:
        """Return a GeoDataFrame representing the intermediate metadata."""
        if not metadata_list:
            raise InvalidOperation("Metadata_list is empty or None. Can not create a GeoDataFrame")

        epsg = metadata_list[0].proj_epsg
        geoms = [m.proj_bbox_as_polygon for m in metadata_list]
        records = convert_fields_to_string(m.to_dict() for m in metadata_list)

        return gpd.GeoDataFrame(records, crs=epsg, geometry=geoms)

    @staticmethod
    def metadata_to_dataframe(metadata_list: List[AssetMetadata]) -> pd.DataFrame:
        """Return a pandas DataFrame representing the intermediate metadata, without the geometry."""
        if not metadata_list:
            raise InvalidOperation("Metadata_list is empty or None. Can not create a GeoDataFrame")

        return pd.DataFrame.from_records(md.to_dict() for md in metadata_list)

    @staticmethod
    def save_geodataframe(gdf: gpd.GeoDataFrame, out_dir: Path, table_name: str) -> None:
        shp_dir = out_dir / "shp"
        if not shp_dir.exists():
            shp_dir.mkdir(parents=True)

        csv_path = out_dir / f"{table_name}.csv"
        shapefile_path = out_dir / f"shp/{table_name}.shp"
        parquet_path = out_dir / f"{table_name}.parquet"

        print(f"Saving pipe-separated CSV file to: {csv_path}")
        gdf.to_csv(csv_path, sep="|")

        print(f"Saving shapefile to: {shapefile_path }")
        gdf.to_file(shapefile_path)

        print(f"Saving geoparquet to: {parquet_path}")
        gdf.to_parquet(parquet_path)


# ##############################################################################
# CLI command-style functions
# ==============================================================================
# The functions below are helper functions to keep the CLI in __main__.py
# as thin and dumb as reasonably possible.
#
# We want to the logic out of the CLI, therefore we put it in these functions
# and the CLI only does the argument parsing, via the click library.
#
# The main advantage is that this style allows for unit tests on core
# functionality of the CLI, and that is harder to do directly on the CLI.
#
# TODO: move the command functions to separate module, perhaps "cli.py"
# ##############################################################################


class CommandsNewPipeline:
    """Putting the new versions of the command under this class for now to
    make switching between old and new easier for testing the conversion.
    This is temporary. Going to move the commands to a separate module.
    """

    @staticmethod
    def build_collection(
        collection_config_path: Path,
        glob: str,
        input_dir: Path,
        output_dir: Path,
        overwrite: bool,
        max_files: Optional[int] = -1,
        # save_dataframe: Optional[bool] = False,
    ):
        collection_config_path = Path(collection_config_path).expanduser().absolute()
        coll_cfg = CollectionConfig.from_json_file(collection_config_path)
        file_coll_cfg = FileCollectorConfig(input_dir=input_dir, glob=glob, max_files=max_files)
        pipeline = GeoTiffPipeline.from_config(
            collection_config=coll_cfg,
            file_coll_cfg=file_coll_cfg,
            output_dir=output_dir,
            overwrite=overwrite,
        )

        pipeline.build_collection()

        # if save_dataframe:
        #     df = pipeline.get_metadata_as_geodataframe()
        #     out_dir = Path("tmp/visualization") / coll_cfg.collection_id
        #     _save_geodataframe(df, out_dir, "metadata_table")

    @staticmethod
    def build_grouped_collections(
        collection_config_path: Path,
        glob: str,
        input_dir: Path,
        output_dir: Path,
        overwrite: bool,
        max_files: Optional[int] = -1,
        # save_dataframe: Optional[bool] = False,
    ):
        collection_config_path = Path(collection_config_path).expanduser().absolute()
        coll_cfg = CollectionConfig.from_json_file(collection_config_path)
        file_coll_cfg = FileCollectorConfig(input_dir=input_dir, glob=glob, max_files=max_files)
        pipeline = GeoTiffPipeline.from_config(
            collection_config=coll_cfg,
            file_coll_cfg=file_coll_cfg,
            output_dir=output_dir,
            overwrite=overwrite,
        )

        pipeline.build_grouped_collections()

        # if save_dataframe:
        #     df = pipeline.get_metadata_as_geodataframe()
        #     out_dir = Path("tmp/visualization") / coll_cfg.collection_id
        #     _save_geodataframe(df, out_dir, "metadata_table")

    @staticmethod
    @staticmethod
    def list_input_files(
        glob: str,
        input_dir: Path,
        max_files: Optional[int] = -1,
    ):
        """Build a STAC collection from a directory of geotiff files."""

        collector = FileCollector()
        collector.input_dir = Path(input_dir)
        collector.glob = glob
        collector.max_files = max_files
        collector.collect()

        for file in collector.input_files:
            print(file)

    @staticmethod
    def list_metadata(
        collection_config_path: Path,
        glob: str,
        input_dir: Path,
        max_files: Optional[int] = -1,
        save_dataframe: bool = True,
    ):
        """Build a STAC collection from a directory of geotiff files."""

        collection_config_path = Path(collection_config_path).expanduser().absolute()
        coll_cfg = CollectionConfig.from_json_file(collection_config_path)
        file_coll_cfg = FileCollectorConfig(input_dir=input_dir, glob=glob, max_files=max_files)
        pipeline = GeoTiffPipeline.from_config(collection_config=coll_cfg, file_coll_cfg=file_coll_cfg)

        if pipeline.has_grouping:
            for group, metadata_list in sorted(pipeline.get_metadata_groups().items()):
                print(f"=== group={group} ===")
                print(f"   number of assets: {len(metadata_list)}")

                for meta in metadata_list:
                    report = {"group": group, "metadata": meta.to_dict(include_internal=True)}
                    pprint.pprint(report)
                    print()
                print()
        else:
            for meta in pipeline.get_metadata():
                pprint.pprint(meta.to_dict(include_internal=True))
                print()

        if save_dataframe:
            df = pipeline.get_metadata_as_geodataframe()
            out_dir = Path("tmp/visualization") / coll_cfg.collection_id
            GeodataframeExporter.save_geodataframe(df, out_dir, "metadata_table")

    @staticmethod
    def list_stac_items(
        collection_config_path: Path,
        glob: str,
        input_dir: Path,
        max_files: Optional[int] = -1,
        save_dataframe: bool = True,
    ):
        """Build a STAC collection from a directory of geotiff files."""

        collection_config_path = Path(collection_config_path).expanduser().absolute()
        coll_cfg = CollectionConfig.from_json_file(collection_config_path)

        file_coll_cfg = FileCollectorConfig(input_dir=input_dir, glob=glob, max_files=max_files)

        pipeline = GeoTiffPipeline.from_config(
            collection_config=coll_cfg, file_coll_cfg=file_coll_cfg, output_dir=None, overwrite=False
        )

        stac_items = list(pipeline.collect_stac_items())
        files = list(pipeline.get_input_files())
        num_itemst = len(stac_items)
        for i, item in enumerate(stac_items):
            if item:
                pprint.pprint(item.to_dict())
            else:
                file = files[i]
                print(
                    f"Received None for a STAC Item {i+1} of {num_itemst}. "
                    + f"Item could not be generated for file: {file}"
                )

        if save_dataframe:
            df = pipeline.get_stac_items_as_geodataframe()
            out_dir = Path("tmp/visualization") / coll_cfg.collection_id
            GeodataframeExporter.save_geodataframe(df, out_dir, "stac_items")

    @staticmethod
    def postprocess_collection(
        collection_file: Path,
        collection_config_path: Path,
        output_dir: Optional[Path] = None,
    ):
        """Run only the post-processing step, on an existing STAC collection.

        Mainly intended to troubleshoot the postprocessing so you don't have to
        regenerate the entire set every time.
        """
        collection_config_path = Path(collection_config_path).expanduser().absolute()
        coll_cfg = CollectionConfig.from_json_file(collection_config_path)

        postprocessor = PostProcessSTACCollectionFile(collection_overrides=coll_cfg.overrides)
        postprocessor.process_collection(collection_file=collection_file, output_dir=output_dir)

    @staticmethod
    def load_collection(
        collection_file: Path,
    ):
        """Show the STAC collection in 'collection_file'."""
        collection = Collection.from_file(collection_file)
        pprint.pprint(collection.to_dict(), indent=2)

    @staticmethod
    def validate_collection(
        collection_file: Path,
    ):
        """Validate a STAC collection."""
        collection = Collection.from_file(collection_file)
        collection.validate_all()
