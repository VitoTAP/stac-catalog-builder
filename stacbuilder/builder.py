import abc
import datetime as dt
import json
import logging
import pprint
import shutil
import tempfile
from itertools import islice
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Optional, Union, Protocol


import geopandas as gpd
import pandas as pd
import pydantic
import pystac
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
import rio_stac.stac as rst


from stacbuilder.pathparsers import (
    InputPathParser,
    InputPathParserFactory,
)
from stacbuilder.config import AssetConfig, CollectionConfig
from stacbuilder.metadata import Metadata
from stacbuilder.timezoneformat import TimezoneFormatConverter


_logger = logging.getLogger(__name__)


CLASSIFICATION_SCHEMA = "https://stac-extensions.github.io/classification/v1.0.0/schema.json"


class SettingsInvalid(Exception):
    pass


from enum import IntEnum


class ProcessingLevels(IntEnum):
    """How far in the processing pipeline you want to go.
    For checking whether all settings are set, because not every step needs
    every parameter (input dir, glob, output dir, collection config, etc. ).

    TODO: Find a better solution for validating the parameters. This is clunky.
    """

    COLLECT_INPUTS = 1
    """Collect input files """

    COLLECT_METADATA = 2
    """Collect STAC item metadata"""

    CREATE_COLLECTION = 3
    """Create a STAC collection"""

    POST_PROCESS = 4
    """Run post processing after a STAC collection was created."""


class InvalidOperation(Exception):
    """Raised when some state or settings are not set, and the operation can not be executed."""

    pass


class STACBuilder:
    """Builds a STAC collections for a dataset of GeoTIFF files in a directory.

    At present: only for a file system, not object storage (yet).
    Working on a more flexible solution.
    """

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

    def __init__(self, path_parser: Optional[InputPathParser] = None):
        self.input_dir: Path = None
        self.glob: str = "*"
        self.max_files_to_process: int = -1

        self._output_dir: Path = None
        self.overwrite: bool = False

        self._path_parser: InputPathParser = path_parser
        self._read_href_modifier: Callable = None

        self._collection_config: CollectionConfig = None

        self._input_files: List[Path] = []
        self._collection: Collection = None

    @property
    def output_dir(self) -> Path:
        """The directory where we should save "collection.json.

        (and the items as well, but those tend to be in subdirectories)
        """
        return self._output_dir

    @output_dir.setter
    def output_dir(self, dir_path: Union[Path, str]) -> Path:
        self._output_dir = Path(dir_path)

    @property
    def collection_overrides(self) -> Dict[str, Any]:
        """Get some key-value pairs that we want to set to a fixed value in the STAC collection.

        To set keys deeper down the dictionary use the notation like a file path:
        {
            "key1/key2/key3: "foo"
        }
        This will only set the value of key3 and keep the values of sibling of
        key3, key2 and off course key1 as well.
        """
        return self.collection_config.overrides or {}

    def build_collection(self):
        """Build the STAC collection and save it to file."""
        self.validate_builder_settings()

        # TODO: generalize this to "collect_metadata", even if it does not come from geotiff file directly
        #   for example: support start from rio-stac or other basic or incomplete STAC items,
        #   start from terracatalogueclient (HRVPP), or support netCDFs.
        # We would have several implementation and use Dependency injection to instantiate the one we need.
        print("Collecting input files ...")
        self.collect_input_files()

        print("Creating STAC collection ...")
        self.create_collection()
        print("Saving STAC collection ...")
        self.save_collection()
        print("Validating STAC collection ...")
        self.validate_collection(self.collection)

        print("Post-processing STAC collection ...")
        self.post_process_collection(self.collection_file)

        print("Saving geodataframe for the bounding boxes of the STAC items ...")
        df = self.get_stac_items_as_geodataframe()
        out_dir = _visualization_dir(self.collection)
        _save_geodataframe(df, out_dir, "stac_items")

        print("DONE")

    def post_process_collection(self, collection_file: Path, output_dir: Optional[Path] = None):
        out_dir: Path = output_dir or collection_file.parent
        new_coll_file, _ = self._create_post_proc_directory_structure(collection_file, out_dir)
        self._convert_timezones_encoded_as_z(collection_file, out_dir)

        self._override_collection_components(new_coll_file)

        # Check if the new file is still valid STAC.
        self.validate_collection(Collection.from_file(new_coll_file))

    def _create_post_proc_directory_structure(self, collection_file: Path, output_dir: Optional[Path] = None):
        in_place = False
        if not output_dir:
            in_place = True
        elif output_dir.exists() and output_dir.samefile(collection_file.parent):
            in_place = True

        # converted_out_dir: Path = output_dir or collection_file.parent
        item_paths = self.get_item_paths_for_coll_file(collection_file)

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

    @property
    def item_paths(self) -> List[Path]:
        """Get file paths for each STAC item in the collection."""
        return self.get_item_paths_for_collection(self.collection)

    def _convert_timezones_encoded_as_z(self, collection_file: Path, output_dir: Path):
        print("Converting UTC timezones encoded as 'Z' to +00:00...")
        conv = TimezoneFormatConverter()
        out_dir = output_dir or collection_file.parent
        item_paths = self.get_item_paths_for_coll_file(collection_file)
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

    @property
    def collection_config(self) -> CollectionConfig:
        return self._collection_config

    @collection_config.setter
    def collection_config(self, config: CollectionConfig) -> None:
        self._collection_config = config

    def collect_input_files(self) -> List[Path]:
        """Find all GeoTIFF files in the directory."""
        input_files = (f for f in self.input_dir.glob(self.glob) if f.is_file())

        if self.max_files_to_process > 0:
            input_files = islice(input_files, self.max_files_to_process)

        self._input_files = input_files
        return self._input_files

    @property
    def input_files(self) -> List[Path]:
        return self._input_files or []

    @property
    def collection(self) -> Collection:
        return self._collection

    @property
    def collection_file(self) -> Path:
        return self.output_dir / "collection.json"

    @property
    def collection_id(self):
        return self._collection_config.collection_id

    @property
    def collection_title(self):
        return self._collection_config.title

    @property
    def collection_description(self):
        return self._collection_config.title

    @property
    def providers(self):
        return [p.to_provider() for p in self._collection_config.providers]

    def _get_input_path_parser(self) -> InputPathParser:
        """Instantiate the parser that extracts metadata from the file path of GeoTIFFS."""
        if not self._path_parser:
            parser_config = self._collection_config.input_path_parser
            self._path_parser = InputPathParserFactory.from_config(parser_config)
        return self._path_parser

    def get_settings_errors(self, level: ProcessingLevels = ProcessingLevels.POST_PROCESS):
        """Check the configuration, but don't raise exceptions. Just list the errors."""
        errors = []

        if level >= ProcessingLevels.COLLECT_INPUTS:
            if not self.input_dir:
                errors.append(f"input_dir is not set")
            elif not self.input_dir.exists():
                errors.append(f"Input directory does not exist: {self.input_dir!r}")

            if not self.glob:
                errors.append(f'glob pattern is not set, default should be "*"')

        if level >= ProcessingLevels.COLLECT_METADATA:
            if not self.collection_config:
                errors.append(f"collection_config is not set")

        if level >= ProcessingLevels.CREATE_COLLECTION:
            if not self.output_dir:
                errors.append(f"output_dir is not set")
            elif self.output_dir.exists() and not self.overwrite:
                errors.append(f"Output directory already exist but overwrite is OFF (False): {self.output_dir}")

        return errors

    def can_run(self, level: ProcessingLevels = ProcessingLevels.POST_PROCESS) -> bool:
        return not self.get_settings_errors(level)

    def validate_builder_settings(self, level: ProcessingLevels = ProcessingLevels.POST_PROCESS) -> None:
        """Raise SettingsInvalid if and configuration errors are found."""
        errors = self.get_settings_errors(level)
        if errors:
            raise SettingsInvalid("\n".join(errors))

    def try_parse_metadata(self):
        """Parse each file into a Metadata object, without creating an collection.

        This is a utility method for troubleshooting.
        """
        self.validate_builder_settings(level=ProcessingLevels.COLLECT_METADATA)
        extract_href_info = self._get_input_path_parser()
        for file in self.input_files:
            metadata = Metadata(
                href=str(file),
                extract_href_info=extract_href_info,
                read_href_modifier=self._read_href_modifier,
            )
            yield metadata

    def try_parse_items(self):
        """Parse each file into a pystac.Item, without creating an collection.

        This is a utility method for troubleshooting.
        """
        self.validate_builder_settings(level=ProcessingLevels.COLLECT_METADATA)
        for file in self.input_files:
            item = self.create_item(file)
            # Skip the yield when we found spurious file that are not items that
            # we know from the collection configuration.
            if item:
                yield item

    def get_stac_items_as_geodataframe(self) -> gpd.GeoDataFrame:
        item_list = list(self.get_stac_items())
        if not item_list:
            raise InvalidOperation("There are no STAC items. Can not create a GeoDataFrame")

        epsg = item_list[0].properties.get("proj:epsg", 4326)
        records = convert_fields_to_string(i.to_dict() for i in item_list)
        shapes = [shape(item.geometry) for item in item_list]
        return gpd.GeoDataFrame(records, crs=epsg, geometry=shapes)

    def get_stac_items(self) -> Iterable[pystac.Item]:
        if not self.collection:
            return []
        return self.collection.get_all_items()

    def create_collection(self):
        """Create a empty pystac.Collection for the dataset."""
        self.validate_builder_settings(level=ProcessingLevels.CREATE_COLLECTION)
        self.validate_builder_settings()
        self._create_collection()

        for file in self.input_files:
            item = self.create_item(file)
            if item is not None:
                item.validate()
                self._collection.add_item(item)

        self._collection.update_extent_from_items()

        # # Show some debug output for the projections
        # # TODO: make the logging work so we can log error warning info and debug seperate.
        # #   Need to do some setup to have logging work accros multiple python modules.
        # proj_bounds = [it.properties.get("proj:bbox") for it in self._collection.get_all_items()]
        # proj_bounds = [p for p in proj_bounds if p is not None]
        # print(f"{proj_bounds=}")

        # epsg_set = set(it.properties.get("proj:epsg") for it in self._collection.get_all_items())
        # epsg_set = set(p for p in epsg_set if p is not None)
        # if not len(epsg_set) == 1:
        #     print(f"WARNING: Item CRSs should all be the same but different codes were found {epsg_set=}")
        # epsg = list(epsg_set)[0]

        # min_x = min(p[0] for p in proj_bounds)
        # min_y = min(p[1] for p in proj_bounds)
        # max_x = max(p[2] for p in proj_bounds)
        # max_y = max(p[3] for p in proj_bounds)
        # coll_proj_bbox = [min_x, min_y, max_x, max_y]
        # print(f"{coll_proj_bbox=}")

        # bbox_lat_lon = reproject_bounding_box(min_x, min_y, max_x, max_y, from_crs=epsg, to_crs="epsg:4326")
        # print(f"{bbox_lat_lon=}")

        layout_template = self.collection_config.layout_strategy_item_template
        strategy = TemplateLayoutStrategy(item_template=layout_template)

        output_dir_str = str(self.output_dir)
        if output_dir_str.endswith("/"):
            output_dir_str = output_dir_str[-1]
        self._collection.normalize_hrefs(output_dir_str, strategy=strategy)

        return self._collection

    def validate_collection(self, collection: Collection):
        """Run STAC validation on the collection."""
        try:
            num_items_validated = collection.validate_all(recursive=True)
        except STACValidationError as exc:
            print(exc)
            raise
        else:
            print(f"Collection valid: number of items validated: {num_items_validated}")

    def save_collection(self) -> Path:
        """Save the STAC collection to file."""
        _logger.info("Saving files ...")
        self._collection.save(catalog_type=CatalogType.SELF_CONTAINED)
        return self.collection_file

    def load_collection(self, path: Path) -> Collection:
        self._collection = Collection.from_file(path)
        return self._collection

    def _load_collection_as_dict(self, coll_file: Path) -> dict:
        with open(coll_file, "r") as f_in:
            return json.load(f_in)

    def _save_collection_as_dict(self, data: Dict[str, Any], coll_file: Path) -> None:
        with open(coll_file, "w") as f_out:
            json.dump(data, f_out, indent=2)

    def _create_collection(self) -> Collection:
        """Creates a STAC Collection.

        Helper method for internal use.
        """

        coll_config: CollectionConfig = self._collection_config
        collection = Collection(
            id=self.collection_id,
            title=coll_config.title,
            description=coll_config.description,
            keywords=coll_config.keywords,
            providers=self.providers,
            extent=self.DEFAULT_EXTENT,
            # summaries=constants.SUMMARIES,
        )
        # TODO: Add support for summaries.
        self._collection = collection

        item_assets_ext = ItemAssetsExtension.ext(collection, add_if_missing=True)
        item_assets_ext.item_assets = self.get_item_assets_definitions()

        RasterExtension.add_to(collection)
        collection.stac_extensions.append(CLASSIFICATION_SCHEMA)

        # TODO: Add support for links in the collection.
        ## collection.add_links(
        ##     [
        ##         constants.PRODUCT_FACT_SHEET,
        ##         constants.PROJECT_WEBSITE,
        ##     ]
        ## )

        return collection

    @property
    def item_assets_configs(self) -> Dict[str, AssetConfig]:
        return self.collection_config.item_assets or {}

    def get_item_assets_definitions(self) -> List[AssetDefinition]:
        asset_definitions = {}
        for band_name, asset_config in self.item_assets_configs.items():
            asset_def: AssetDefinition = asset_config.to_asset_definition()
            asset_def.owner = self.collection
            asset_definitions[band_name] = asset_def

        return asset_definitions

    def create_item(
        self,
        tiff_path: Path,
        # read_href_modifier: Optional[ReadHrefModifier] = None,
        # extract_href_info: Optional[Callable[[str], dict]] = None,
    ) -> Item:
        """Create a STAC Item with one or two assets.

        Args:
            read_href_modifier (Callable[[str], str]): An optional function to
                modify the MTL and USGS STAC hrefs (e.g. to add a token to a url).
            raster_footprint (bool): Flag to use the footprint of valid (not nodata)

        Returns:
            Item: STAC Item object representing the forest carbon monitoring tile
        """
        extract_href_info = self._get_input_path_parser()
        metadata = Metadata(
            href=str(tiff_path),
            extract_href_info=extract_href_info,
            read_href_modifier=self._read_href_modifier,
        )

        if metadata.item_type not in self.item_assets_configs:
            _logger.warning(
                "Found an unknown item type, not defined in collection configuration: "
                f"{metadata.item_type}, returning item=None"
            )
            return None

        item = Item(
            href=metadata.href,
            id=metadata.item_id,
            geometry=metadata.geometry,
            bbox=metadata.bbox,
            datetime=metadata.datetime,
            start_datetime=metadata.start_datetime,
            end_datetime=metadata.end_datetime,
            properties={
                "product_version": metadata.version,
                # "product_tile": metadata.tile,
            },
        )

        description = self.item_assets_configs[metadata.item_type].description
        item.common_metadata.description = description
        # item.common_metadata.description = self.collection_description

        item.common_metadata.created = dt.datetime.utcnow()

        # item.common_metadata.mission = constants.MISSION
        # item.common_metadata.platform = constants.PLATFORM
        # item.common_metadata.instruments = constants.INSTRUMENTS

        item.add_asset(metadata.item_type, self.create_asset(metadata))

        item_proj = ItemProjectionExtension.ext(item, add_if_missing=True)
        item_proj.epsg = metadata.proj_epsg
        item_proj.bbox = metadata.proj_bbox
        item_proj.geometry = metadata.proj_geometry
        item_proj.transform = metadata.transform
        item_proj.shape = metadata.shape

        # grid = GridExtension.ext(item, add_if_missing=True)
        # grid.code = f"TILE-{metadata.tile}"

        RasterExtension.add_to(item)
        item.stac_extensions.append(CLASSIFICATION_SCHEMA)

        return item

    def create_asset(self, metadata: Metadata) -> Asset:
        asset_defs = self.get_item_assets_definitions()
        asset_def: AssetDefinition = asset_defs[metadata.item_type]
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

    def get_item_from_rio_stac(
        self,
        tiff_path: Path,
    ):
        """Creates a STAC item from a GeoTIFF file, using rio-stac.

        This is the equivalent of the command `rio stac`.
        """
        return rst.create_stac_item(
            source=str(tiff_path),
            collection=self.collection_id,
            collection_url=str(self.collection_file),
        )


###############################################################################
# The STACBuilder above is getting complicated.
# Working towards a simplifying it, with some pluggable parts,
#  and not so many attributes/data to keep.
# Not all the classes below will stick around. Trying out what works best.
###############################################################################


class FileCollectorConfig(pydantic.BaseModel):
    input_dir: Path
    glob: Optional[str] = "*"
    max_files: int = -1


class IDataCollector(Protocol):
    """Interface for all DataCollector implementations."""

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
            self,
            input_dir: Optional[Path] = None,
            glob: Optional[str] = "*",
            max_files: Optional[int] = -1
        ) -> None:

        #
        # Settings
        #
        # These are public members, or should have a public property.
        self.input_dir: Path = input_dir
        self.glob: str = glob
        self.max_files: int = max_files
        self._unify_not_set_to_defaults()

        #
        # Internal state below
        #

        # The result:
        self._input_files: List[Path] = None

    def _unify_not_set_to_defaults(self):
        if not self.input_dir:
            self.input_dir = None

        if not self.glob:
            self.glob = "*"

        if not self.max_files:
            self.max_files = -1

    @staticmethod
    def from_config(config: FileCollectorConfig) -> "FileCollector":
        collector = FileCollector()
        collector.setup(config)
        return collector

    def setup(self, config: FileCollectorConfig):
        """Use the configuration to set up this FileCollector."""
        self.input_dir = config.input_dir
        self.glob = config.glob
        self.max_files = config.max_files
        self._unify_not_set_to_defaults()
        self.reset()

    def collect(self):
        input_files = (f for f in self.input_dir.glob(self.glob) if f.is_file())

        if self.max_files > 0:
            input_files = islice(input_files, self.max_files)

        self._input_files = input_files

    def has_collected(self) -> bool:
        return self._input_files is not None

    def reset(self):
        self._input_files = None

    @property
    def input_files(self) -> List[Path]:
        """Get the collected input files."""
        return self._input_files or []


class IMetadataCollector(IDataCollector):
    """Protocol for collector that gets Metadata objects from a source.

    You need still to implement the method `collect`
    """

    def __init__(self):
        self._metadata_list: List[Metadata] = None

    def has_collected(self) -> bool:
        return self._metadata_list is not None

    def reset(self):
        self._metadata_list = None

    @property
    def metadata(self) -> List[Metadata]:
        return self._metadata_list or []


class ISTACItemCollector(IDataCollector):
    """Protocol for collector that gets STAC Items from a source.

    You need still to implement the method `collect`
    """

    def __init__(self):
        self._metadata_list: List[Metadata] = None

    def has_collected(self) -> bool:
        return self._metadata_list is not None

    def reset(self):
        self._metadata_list = None

    @property
    def stac_items(self) -> List[Item]:
        return self._metadata_list or []


# TODO: we can probably eliminate this one.
# class MapGeoTiffToMetaData:
#     """Takes a path to a GeoTIFF and extract the Metadata for that file.

#     Like a node in a pipeline.
#     Intention is to be able to use generators with this processor, to
#     parse large datasets more efficiently.
#     """

#     def __init__(self, path_parser: InputPathParser) -> None:
#         self._path_parser = path_parser

#     def map(self, file: Path) -> Metadata:
#         """Extract the metadata for the specified GeoTIFF path."""
#         return Metadata(
#             href=str(file),
#             extract_href_info=self._path_parser,
#             read_href_modifier=None,
#         )

#     def map_all(self, files: Iterable[Path]) -> Iterable[Metadata]:
#         """Return generator the converts all files to Metadata objects"""
#         return (self.map(file) for file in files)


class IMapMetadataToSTACItem(Protocol):
    """Like a mapping, converts intermediate Metadata objects STAC Items."""

    def map(self, metadata: Metadata) -> Item:
        """Converts a Metadata objects to a STAC Items."""
        ...

    def map_all(self, metadata_source: Iterable[Metadata]) -> Iterable[Item]:
        """Return generator the converts all metadata objects to STAC Items"""
        return (self.map(metadata) for metadata in metadata_source)


class MapMetadataToSTACItem(IMapMetadataToSTACItem):

    # def __init__(self, collection_config: CollectionConfig) -> None:
    #     super().__init__()
    #     self._collection_config = collection_config

    def __init__(self, item_assets_configs: Dict[str, AssetConfig]) -> None:
        super().__init__()
        self._item_assets_configs: item_assets_configs = item_assets_configs

    # @property
    # def collection_config(self) -> CollectionConfig:
    #     return self._collection_config

    # @property
    # def item_assets_configs(self) -> Dict[str, AssetConfig]:
    #     return self.collection_config.item_assets or {}

    @property
    def item_assets_configs(self) ->  Dict[str, AssetConfig]:
        return self._item_assets_configs

    def get_item_assets_definitions(self) -> List[AssetDefinition]:
        asset_definitions = {}
        for band_name, asset_config in self.item_assets_configs.items():
            asset_def: AssetDefinition = asset_config.to_asset_definition()
            # asset_def.owner = self.collection
            asset_definitions[band_name] = asset_def

        return asset_definitions

    def map(self, metadata: Metadata) -> Item:
        if metadata.item_type not in self.item_assets_configs:
            _logger.warning(
                "Found an unknown item type, not defined in collection configuration: "
                f"{metadata.item_type}, returning item=None"
            )
            return None

        item = Item(
            href=metadata.href,
            id=metadata.item_id,
            geometry=metadata.geometry,
            bbox=metadata.bbox,
            datetime=metadata.datetime,
            start_datetime=metadata.start_datetime,
            end_datetime=metadata.end_datetime,
            properties={
                "product_version": metadata.version,
                # "product_tile": metadata.tile,
            },
        )

        description = self.item_assets_configs[metadata.item_type].description
        item.common_metadata.description = description
        # item.common_metadata.description = self.collection_description

        item.common_metadata.created = dt.datetime.utcnow()

        # item.common_metadata.mission = constants.MISSION
        # item.common_metadata.platform = constants.PLATFORM
        # item.common_metadata.instruments = constants.INSTRUMENTS

        item.add_asset(metadata.item_type, self.create_asset(metadata))

        item_proj = ItemProjectionExtension.ext(item, add_if_missing=True)
        item_proj.epsg = metadata.proj_epsg
        item_proj.bbox = metadata.proj_bbox
        item_proj.geometry = metadata.proj_geometry
        item_proj.transform = metadata.transform
        item_proj.shape = metadata.shape

        # grid = GridExtension.ext(item, add_if_missing=True)
        # grid.code = f"TILE-{metadata.tile}"

        RasterExtension.add_to(item)
        item.stac_extensions.append(CLASSIFICATION_SCHEMA)

        return item

    def create_asset(self, metadata: Metadata) -> Asset:
        asset_defs = self.get_item_assets_definitions()
        asset_def: AssetDefinition = asset_defs[metadata.item_type]
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


class MapGeoTiffToSTACItem:
    """Like a mapping, extract STAC Items from each file."""

    def __init__(
            self,
            path_parser: InputPathParser,
            map_metadata_to_stac_item: IMapMetadataToSTACItem
        ) -> None:
        self._path_parser = path_parser
        self._metadata_to_stac_item = map_metadata_to_stac_item

    def to_stac_item(self, file: Path) -> Item:
        """Generate the STAC Item for the specified GeoTIFF path."""
        metadata = self.to_metadata(file)
        return self._metadata_to_stac_item.map(metadata)

    def to_metadata(self, file: Path) -> Metadata:
        """Generate the intermediate Metadata for the specified GeoTIFF path."""
        return Metadata(
            href=str(file),
            extract_href_info=self._path_parser,
            read_href_modifier=None,
        )

    def map_all(self, files: Iterable[Path]) -> Iterable[Metadata]:
        """Return generator the converts all files to STAC Items"""
        return (self.to_stac_item(file) for file in files)


# TODO: not yet used, might want to remove it ASAP so we can simplify the code.
# class GeoTiffMetadataCollector(IMetadataCollector):
#     """Collects Metadata objects for GeoTIFF files.

#     This is closer to the original design and a bit simpler to implement,
#     but not as efficient or flexible.
#     Probably won't keep this class.
#     """

#     def __init__(self, file_collector: FileCollector, path_parser: InputPathParser):
#         super().__init__()

#         # Components we depend on: store injected dependencies.
#         self._file_collector = file_collector
#         self._path_parser = path_parser

#         # Create internal components.
#         self._processor = MapGeoTiffToMetaData(path_parser)

#     def can_run(self):
#         """Check if we are ready to run: have the components we depend on been set up or not."""
#         if not isinstance(self._file_collector, FileCollector):
#             return False
#         if not isinstance(self._path_parser, InputPathParser):
#             return False
#         return True

#     def pre_run_check(self):
#         """Raise an error if it can not run due to an incorrect set up."""
#         if not self.can_run():
#             raise InvalidOperation(
#                 f"Can not run, set up is not correct for {self.__class__.__name__}"
#                 + "check _file_collector and _path_parser"
#             )

#     @property
#     def input_files(self):
#         """Get the collected input files."""
#         return self._file_collector.input_files

#     def collect(self):
#         self.pre_run_check()
#         self.reset()
#         self._file_collector.collect()
#         self._metadata_list = []

#         for file in self.input_files:
#             metadata = self._processor.map(file)
#             self._metadata_list.append(metadata)


# class GeoTiffToSTACItem:
#     """Takes a path to a GeoTIFF and extract the STAC Item for that file.

#     Like a node in a pipeline.
#     Intention is to be able to use generators with this processor, to
#     parse large datasets more efficiently.
#     """

#     def __init__(
#         self, file_collector: FileCollector, path_parser: InputPathParser, item_assets_configs: Dict[str, AssetConfig]
#     ) -> None:
#         # Components we depend on: store injected dependencies.
#         self._file_collector = file_collector
#         self._path_parser = path_parser
#         self._item_assets_configs = item_assets_configs

#     # def setup(self, collection_config: CollectionConfig, file_coll_cfg: FileCollectorConfig):
#     #     """Set up the object with new components, as specified in the configuration objects."""
#     #     # TODO: move this kind of setup / pipeline creation (factories) to pipeline class.
#     #     self._path_parser = InputPathParserFactory.from_config(collection_config.input_path_parser)
#     #     self._file_collector = FileCollector()
#     #     self._file_collector.setup(file_coll_cfg)

#     def process(self, file: Path) -> Metadata:
#         """Create a STAC item for the specified GeoTIFF path."""
#         metadata = Metadata(
#             href=str(file),
#             extract_href_info=self._path_parser,
#             read_href_modifier=None,
#         )

#         if metadata.item_type not in self._item_assets_configs:
#             _logger.warning(
#                 "Found an unknown item type, not defined in collection configuration: "
#                 f"{metadata.item_type}, returning item=None"
#             )
#             return None

#         assert metadata.item_id is not None

#         item = Item(
#             href=metadata.href,
#             id=metadata.item_id,
#             geometry=metadata.geometry,
#             bbox=metadata.bbox,
#             datetime=metadata.datetime,
#             start_datetime=metadata.start_datetime,
#             end_datetime=metadata.end_datetime,
#             properties={
#                 "product_version": metadata.version,
#                 # "product_tile": metadata.tile,
#             },
#         )
#         # TODO: support optional parts: store the tile ID if dataset uses that.
#         #   We would need a way to customize extracting that tile ID for the specific dataset.

#         description = self._item_assets_configs[metadata.item_type].description
#         item.common_metadata.description = description

#         item.common_metadata.created = dt.datetime.utcnow()

#         # TODO: support optional parts: these fields are recommended but they are also not always relevant or present.
#         # item.common_metadata.mission = constants.MISSION
#         # item.common_metadata.platform = constants.PLATFORM
#         # item.common_metadata.instruments = constants.INSTRUMENTS

#         item.add_asset(metadata.item_type, self._create_asset(metadata))

#         item_proj = ItemProjectionExtension.ext(item, add_if_missing=True)
#         item_proj.epsg = metadata.proj_epsg
#         item_proj.bbox = metadata.proj_bbox
#         item_proj.geometry = metadata.proj_geometry
#         item_proj.transform = metadata.transform

#         # TODO: support optional parts: grid extension is recommended if we are indeed on a grid, but
#         #    that is not always the case.
#         # grid = GridExtension.ext(item, add_if_missing=True)
#         # grid.code = f"TILE-{metadata.tile}"

#         RasterExtension.add_to(item)
#         item.stac_extensions.append(CLASSIFICATION_SCHEMA)

#         return item

#     def _create_asset(self, metadata: Metadata) -> Asset:
#         asset_defs = self._get_item_assets_definitions()
#         asset_def: AssetDefinition = asset_defs[metadata.item_type]
#         return asset_def.create_asset(metadata.href)

#     def _get_item_assets_definitions(self) -> List[AssetDefinition]:
#         asset_definitions = {}
#         for band_name, asset_config in self._item_assets_configs.items():
#             asset_def: AssetDefinition = asset_config.to_asset_definition()
#             # TODO: check wether we do need to store the collection as the owner here.
#             # asset_def.owner = self.collection
#             asset_definitions[band_name] = asset_def

#         return asset_definitions


class STACCollectionBuilder:
    """Creates a collector from collected STAC Items."""

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

    def __init__(
        self,
        collection_config: CollectionConfig,
        # item_assets_configs: Dict[str, AssetConfig],
        # stac_items_collector: ISTACItemCollector,
        # map_metadata_to_stac_item: IMapMetadataToSTACItem,
        output_dir: Path,
        overwrite: bool = False
    ) -> None:
        self._collection_config = collection_config
        # Components we depend on: store injected dependencies.
        # self._item_assets_configs: Dict[str, AssetConfig] = collection_config.item_assets_configs
        # self._stac_items_collector: ISTACItemCollector = stac_items_collector
        # self._map_metadata_to_stac_item = map_metadata_to_stac_item

        # settings: not components, just direct variables
        self._output_dir = Path(output_dir)
        self._overwrite_output = overwrite

        # internal temporary state
        self._stac_items: List[Item] = None

        # result
        self._collection: Collection = None

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

        self._stac_items = list(stac_items) or []
        self.create_collection()
        self.save_collection()

        # We save before we validate, because when the validation fails we want
        # to be able to inspect the incorrect result.
        self.validate_collection(self.collection)

    def create_collection(
        self,
    ):
        """Create a empty pystac.Collection for the dataset."""
        self._create_empty_collection()

        item: Item
        for item in self._stac_items:
            # for asset in item.assets:
            #     asset.owner = self._collection
            self._collection.add_item(item)

        self._collection.update_extent_from_items()

        layout_template = self._collection_config.layout_strategy_item_template
        strategy = TemplateLayoutStrategy(item_template=layout_template)

        output_dir_str = str(self.output_dir)
        if output_dir_str.endswith("/"):
            output_dir_str = output_dir_str[-1]
        self._collection.normalize_hrefs(output_dir_str, strategy=strategy)

        return self._collection

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
            # TODO: check wether we do need to store the collection as the owner here.
            asset_def.owner = self.collection
            asset_definitions[band_name] = asset_def

        return asset_definitions


class GeoTiffPipeline:
    """A pipeline to generate a STAC collection from a directory containing GeoTIFF files."""

    def __init__(
        self,
        collection_config: CollectionConfig,
        file_collector: FileCollector,
        path_parser: InputPathParser,
        # item_assets_configs: Dict[str, AssetConfig],
        output_dir: Path,
        overwrite: bool = False,
    ) -> None:
        # Store dependencies: components that have to be provided to constructor
        self._file_collector = file_collector
        self._path_parser = path_parser
        # self._item_assets_configs = item_assets_configs
        self._collection_config = collection_config

        # components we set up internally
        # self._stac_item_processor: GeoTiffToSTACItem = None
        self._map_meta_to_stac_item: MapMetadataToSTACItem = None
        self._collection_builder: STACCollectionBuilder = None

        # settings, not components
        # TODO: eliminate self.output_dir and self.overwrite
        self.output_dir: Path = output_dir or Path(tempfile.gettempdir())
        self.overwrite: bool = overwrite

        # result
        self._collection: Optional[Collection] = None

    @property
    def item_assets_configs(self) -> Dict[str, AssetConfig]:
        return self._collection_config.item_assets or {}

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
        pipeline = GeoTiffPipeline(None, None, None, None, None)
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

        self._collection_config = collection_config
        self._file_collector = FileCollector.from_config(file_coll_cfg)
        self._path_parser = InputPathParserFactory.from_config(collection_config.input_path_parser)

        self.output_dir = output_dir or Path(tempfile.gettempdir())
        self.overwrite = overwrite
        self._setup_interals()


    def _setup_interals(self) -> None:
        """Setup the internal components based on the components that we receive via dependency injection."""
        if self._collection_builder:
            # It has already been set up => ignore
            return

        # self._stac_item_processor = GeoTiffToSTACItem(
        #     file_collector=self._file_collector,
        #     path_parser=self._path_parser,
        #     item_assets_configs=self.item_assets_configs,
        # )
        self._map_meta_to_stac_item = MapMetadataToSTACItem(
            item_assets_configs=self.item_assets_configs
        )
        self._collection_builder = STACCollectionBuilder(
            collection_config=self._collection_config,
            output_dir=self.output_dir,
            overwrite=self.overwrite
        )

    def get_collection(self) -> Optional[Collection]:
        return self._collection

    def collect_input_files(self) -> Iterable[Path]:
        """Collect the input files for processing."""
        self._file_collector.collect()
        for file in self._file_collector.input_files:
            yield file

    def collect_metadata(self) -> Iterable[Metadata]:
        """Generate the intermediate metadata objects, from the input files."""
        self._file_collector.collect()
        for file in self.collect_input_files():
            yield Metadata(
                href=str(file),
                extract_href_info=self._path_parser,
                read_href_modifier=None,
            )


    def collect_stac_items(self):
        """Generate the intermediate STAC Item objects."""
        self._setup_interals()

        self._file_collector.collect()
        for file in self.collect_input_files():
            # yield self._stac_item_processor.process(file)
            metadata = Metadata(
                href=str(file),
                extract_href_info=self._path_parser,
                read_href_modifier=None,
            )
            yield self._map_meta_to_stac_item.map(metadata)


    def get_metadata_as_geodataframe(self) -> gpd.GeoDataFrame:
        """Return a GeoDataFrame representing the intermediate metadata."""
        meta_list = list(self.collect_metadata())
        if not meta_list:
            raise InvalidOperation("There are no STAC items. Can not create a GeoDataFrame")

        epsg = meta_list[0].proj_epsg
        geoms = [m.proj_geometry_shapely for m in meta_list]
        records = convert_fields_to_string(m.to_dict() for m in meta_list)

        return gpd.GeoDataFrame(records, crs=epsg, geometry=geoms)

    def get_metadata_as_dataframe(self) -> pd.DataFrame:
        """Return a pandas DataFrame representing the intermediate metadata, without the geometry."""
        return pd.DataFrame.from_records(md.to_dict() for md in self.collect_metadata())

    def get_stac_items_as_geodataframe(self) -> gpd.GeoDataFrame:
        """Return a GeoDataFrame representing the STAC Items."""
        item_list = list(self.collect_stac_items())
        if not item_list:
            raise InvalidOperation("There are no STAC items. Can not create a GeoDataFrame")

        epsg = item_list[0].properties.get("proj:epsg", 4326)
        records = convert_fields_to_string(i.to_dict() for i in item_list)
        shapes = [shape(item.geometry) for item in item_list]
        return gpd.GeoDataFrame(records, crs=epsg, geometry=shapes)

    def get_stac_items_as_dataframe(self) -> pd.DataFrame:
        """Return a pandas DataFrame representing the STAC Items, without the geometry."""
        return pd.DataFrame.from_records(md.to_dict() for md in self.collect_stac_items())

    def build_collection(self):
        """Build the entire STAC collection."""
        self._setup_interals()

        # # TODO: eliminate self.output_dir and self.overwrite
        # self._collection_builder.output_dir: Path = self.output_dir
        # self._collection_builder.overwrite: bool = self.overwrite

        self._collection = self._collection_builder.build_collection(self.collect_stac_items())


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


def convert_fields_to_string(records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out_records = [dict(rec) for rec in records]
    for rec in out_records:
        for key, val in rec.items():
            if isinstance(val, dt.datetime):
                rec[key] = val.isoformat()
            elif isinstance(val, list):
                rec[key] = json.dumps(val)
    return out_records


def _setup_builder(
    input_dir: Path,
    glob: str,
    output_dir: Optional[Path] = None,
    overwrite: Optional[bool] = False,
    collection_config_path: Optional[Path] = None,
    max_files_to_process: Optional[int] = -1,
) -> STACBuilder:
    """Build a STAC collection from a directory of geotiff files."""

    builder = STACBuilder()

    if collection_config_path:
        conf_contents = collection_config_path.read_text()
        config = CollectionConfig(**json.loads(conf_contents))
        builder.collection_config = config

    builder.glob = glob
    builder.input_dir = input_dir
    builder.max_files_to_process = max_files_to_process

    builder.output_dir = output_dir
    builder.overwrite = overwrite

    return builder


def command_build_collection(
    collection_config_path: Path,
    glob: str,
    input_dir: Path,
    output_dir: Path,
    overwrite: bool,
    max_files: Optional[int] = -1,
):
    """Build a STAC collection from a directory of geotiff files."""
    builder: STACBuilder = _setup_builder(
        collection_config_path=Path(collection_config_path).expanduser().absolute(),
        glob=glob,
        input_dir=Path(input_dir).expanduser().absolute(),
        output_dir=Path(output_dir).expanduser().absolute(),
        overwrite=overwrite,
        max_files_to_process=max_files,
    )
    builder.build_collection()


def old_command_list_metadata(
    collection_config_path: Path,
    glob: str,
    input_dir: Path,
    max_files: Optional[int] = -1,
    as_dataframe: bool = False,
):
    """Build a STAC collection from a directory of geotiff files."""

    builder: STACBuilder = _setup_builder(
        collection_config_path=Path(collection_config_path).expanduser().absolute(),
        glob=glob,
        input_dir=Path(input_dir).expanduser().absolute(),
        output_dir=Path("/tmp"),
        overwrite=True,
        max_files_to_process=max_files,
    )

    builder.validate_builder_settings()
    builder.collect_input_files()

    if as_dataframe:
        df: gpd.GeoDataFrame = builder.metadata_as_geodataframe()
        print(df.to_string())
        out_dir = Path("metadata_table")
        df.to_csv(out_dir / "metadata_table.csv", sep="|")
        df.to_file(out_dir / "shp/metadata_table.shp")
        df.to_parquet(out_dir / "metadata_table.parquet")
    else:
        metadata: Metadata
        for metadata in builder.try_parse_metadata():
            pprint.pprint(metadata.to_dict(include_internal=True))
            print()


def old_command_list_stac_items(
    collection_config_path: Path,
    glob: str,
    input_dir: Path,
    max_files: Optional[int] = -1,
    as_dataframe: bool = False,
):
    """Build a STAC collection from a directory of geotiff files."""

    builder: STACBuilder = _setup_builder(
        collection_config_path=Path(collection_config_path).expanduser().absolute(),
        glob=glob,
        input_dir=Path(input_dir).expanduser().absolute(),
        output_dir=Path("/tmp"),
        overwrite=True,
        max_files_to_process=max_files,
    )
    builder.collect_input_files()

    if as_dataframe:
        df = builder.items_as_dataframe()
        df.to_csv("stac_items_table.csv", sep="|")
    else:
        min_west = None
        max_east = None
        min_south = None
        max_north = None

        items_bbox = {
            "min_west": None,
            "max_east": None,
            "min_south": None,
            "max_north": None,
        }

        for item in builder.try_parse_items():
            pprint.pprint(item.to_dict())
            print()
            west, south, east, north = item.bbox
            if min_west is None:
                min_west = west
                max_east = east
                min_south = south
                max_north = north
            else:
                if west < min_west:
                    items_bbox["min_west"] = item.id
                    min_west = west
                if south < min_south:
                    items_bbox["min_south"] = item.id
                    min_south = south
                if east < max_east:
                    items_bbox["max_east"] = item.id
                    max_east = east
                if north < max_north:
                    items_bbox["max_north"] = item.id
                    max_north = north

        print(f"Collection BBox: [{min_west=}, {min_south=}, {max_east=}, {max_north=}]")
        print(f"{items_bbox}=]")


def command_load_collection(
    collection_file: Path,
):
    """Show the STAC collection in 'collection_file'."""
    collection = Collection.from_file(collection_file)
    pprint.pprint(collection.to_dict(), indent=2)


def command_validate_collection(
    collection_file: Path,
):
    """Validate a STAC collection."""
    collection = Collection.from_file(collection_file)
    collection.validate_all()


def command_post_process_collection(
    collection_file: Path,
    collection_config_path: Path,
    output_dir: Optional[Path] = None,
):
    """Run only the post-processing step on an existing STAC collection.

    Mainly intended to troubleshoot the postprocessing so you don't have to
    regenerate the entire set every time.
    """
    builder = STACBuilder()
    collection_config_path = Path(collection_config_path)
    conf_contents = collection_config_path.read_text()
    config = CollectionConfig(**json.loads(conf_contents))
    builder.collection_config = config

    out_dir = Path(output_dir) if output_dir else None
    builder.post_process_collection(Path(collection_file), out_dir)


class CommandsNewPipeline:
    """Putting the new versions of the command under this class for now to
    make switching between old and new easier for testing the conversion.
    This is temporary. Going to move the commands to a separate module.
    """

    @staticmethod
    def command_build_collection_newpipe(
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
    def command_list_input_files(
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
    def command_list_metadata(
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

        for meta in pipeline.collect_metadata():
            pprint.pprint(meta.to_dict(include_internal=True))
            print()

        if save_dataframe:
            df = pipeline.get_metadata_as_geodataframe()
            out_dir = Path("tmp/visualization") / coll_cfg.collection_id
            _save_geodataframe(df, out_dir, "metadata_table")

    @staticmethod
    def command_list_stac_items(
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
            collection_config=coll_cfg,
            file_coll_cfg=file_coll_cfg,
            output_dir=None,
            overwrite=False
        )

        stac_items = list(pipeline.collect_stac_items())
        files = list(pipeline.collect_input_files())
        num_itemst = len(stac_items)
        for i, item in enumerate(stac_items):
            if item:
                pprint.pprint(item.to_dict())
            else:
                file = files[i]
                print(
                    f"Received None for a STAC Item {i+1} of {num_itemst}. " +
                    f"Item could not be generated for file: {file}"
                )

        if save_dataframe:
            df = pipeline.get_stac_items_as_geodataframe()
            out_dir = Path("tmp/visualization") / coll_cfg.collection_id
            _save_geodataframe(df, out_dir, "stac_items")


def _visualization_dir(collection: Collection):
    collection_path = Path(collection.self_href)
    return collection_path.parent / "tmp" / "visualization" / collection.id


def _save_geodataframe(gdf: gpd.GeoDataFrame, out_dir: Path, table_name: str) -> None:
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
