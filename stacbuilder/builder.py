import datetime as dt
import json
import logging
import pprint
import shutil


from itertools import islice
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Union

import shapely
import pandas as pd
import pystac

from shapely.geometry import GeometryCollection, MultiPolygon
from pystac import Asset, CatalogType, Collection, Extent, Item, SpatialExtent, TemporalExtent
from pystac.errors import STACValidationError
from pystac.layout import TemplateLayoutStrategy
from pystac.extensions.grid import GridExtension
from pystac.extensions.item_assets import AssetDefinition, ItemAssetsExtension
from pystac.extensions.projection import ItemProjectionExtension, ProjectionExtension
from pystac.extensions.raster import RasterExtension


from stactools.core.io import ReadHrefModifier
import rio_stac.stac as rst


from stacbuilder.core import (
    InputPathParser,
    InputPathParserFactory,
)
from stacbuilder.config import AssetConfig, CollectionConfig
from stacbuilder.metadata import Metadata
from stacbuilder.timezoneformat import TimezoneFormatConverter
from stacbuilder.projections import reproject_bounding_box


_logger = logging.getLogger(__name__)


CLASSIFICATION_SCHEMA = "https://stac-extensions.github.io/classification/v1.0.0/schema.json"


class SettingsInvalid(Exception):
    pass


from enum import IntEnum, auto


class ProcessingLevels(IntEnum):
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
        return self._output_dir

    @output_dir.setter
    def output_dir(self, dir_path: Union[Path, str]) -> Path:
        self._output_dir = Path(dir_path)

    @property
    def collection_overrides(self) -> Dict[str, Any]:
        return self.collection_config.overrides or {}

    def build_collection(self):
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

        # print("Saving GeoJSON file with footprints of the STAC items ...")
        # self.save_footprints()

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

    def _load_collection_as_dict(self, coll_file: Path) -> dict:
        with open(coll_file, "r") as f_in:
            return json.load(f_in)

    def _save_collection_as_dict(self, data: Dict[str, Any], coll_file: Path) -> None:
        with open(coll_file, "w") as f_out:
            json.dump(data, f_out, indent=2)

    @property
    def collection_config(self) -> CollectionConfig:
        return self._collection_config

    @collection_config.setter
    def collection_config(self, config: CollectionConfig) -> None:
        self._collection_config = config

    def collect_input_files(self) -> List[Path]:
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
        if not self._path_parser:
            parser_config = self._collection_config.input_path_parser
            self._path_parser = InputPathParserFactory.from_config(parser_config)
        return self._path_parser

    def get_settings_errors(self, level: ProcessingLevels = ProcessingLevels.POST_PROCESS):
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
        errors = self.get_settings_errors(level)
        if errors:
            raise SettingsInvalid("\n".join(errors))

    def try_parse_metadata(self):
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
        self.validate_builder_settings(level=ProcessingLevels.COLLECT_METADATA)
        for file in self.input_files:
            item = self.create_item(file)
            # Skip the yield when we found spurious file that are not items that
            # we know from the collection configuration.
            if item:
                yield item

    def metadata_as_dataframe(self) -> pd.DataFrame:
        return pd.DataFrame.from_records(md.to_dict() for md in self.try_parse_metadata())

    def items_as_dataframe(self) -> pd.DataFrame:
        return pd.DataFrame.from_records(it.to_dict() for it in self.try_parse_items())

    def create_collection(self):
        self.validate_builder_settings(level=ProcessingLevels.CREATE_COLLECTION)
        self.validate_builder_settings()
        self._create_collection()

        for file in self.input_files:
            item = self.create_item(file)
            if item is not None:
                item.validate()
                self._collection.add_item(item)

        self._collection.update_extent_from_items()

        # Show some debug output for the projections
        # TODO: make the logging work so we can log error warning info and debug seperate.
        #   Need to do some setup to have logging work accros multiple python modules.
        proj_bounds = [it.properties.get("proj:bbox") for it in self._collection.get_all_items()]
        proj_bounds = [p for p in proj_bounds if p is not None]
        print(f"{proj_bounds=}")

        epsg_set = set(it.properties.get("proj:epsg") for it in self._collection.get_all_items())
        epsg_set = set(p for p in epsg_set if p is not None)
        if not len(epsg_set) == 1:
            print(f"WARNING: Item CRSs should all be the same but different codes were found {epsg_set=}")
        epsg = list(epsg_set)[0]

        min_x = min(p[0] for p in proj_bounds)
        min_y = min(p[1] for p in proj_bounds)
        max_x = max(p[2] for p in proj_bounds)
        max_y = max(p[3] for p in proj_bounds)
        coll_proj_bbox = [min_x, min_y, max_x, max_y]
        print(f"{coll_proj_bbox=}")

        bbox_lat_lon = reproject_bounding_box(min_x, min_y, max_x, max_y, from_crs=epsg, to_crs="epsg:4326")
        print(f"{bbox_lat_lon=}")

        layout_template = self.collection_config.layout_strategy_item_template
        strategy = TemplateLayoutStrategy(item_template=layout_template)

        output_dir_str = str(self.output_dir)
        if output_dir_str.endswith("/"):
            output_dir_str = output_dir_str[-1]
        self._collection.normalize_hrefs(output_dir_str, strategy=strategy)

        return self._collection

    def validate_collection(self, collection: Collection):
        try:
            num_items_validated = collection.validate_all(recursive=True)
        except STACValidationError as exc:
            print(exc)
            raise
        else:
            print(f"Collection valid: number of items validated: {num_items_validated}")

    @property
    def footprints_path(self) -> Path:
        coll_path = self.collection_file
        return coll_path.parent / "footprints.json"

    def save_footprints(self) -> Path:
        item_coll = pystac.ItemCollection(items=self._collection.get_all_items())
        item_coll.save_object(str(self.footprints_path))
        return self.footprints_path

    def save_collection(self) -> Path:
        _logger.info("Saving files ...")
        self._collection.save(catalog_type=CatalogType.SELF_CONTAINED)
        return self.collection_file

    def _create_collection(self) -> Collection:
        """Creates a STAC Collection."""

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

    def load_collection(self, path: Path) -> Collection:
        self._collection = Collection.from_file(path)
        return self._collection

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

        assert metadata.item_id is not None

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
        item_proj.epsg = metadata.proj_epsg.to_epsg()
        item_proj.bbox = metadata.proj_bbox
        item_proj.geometry = metadata.proj_geometry
        item_proj.transform = metadata.transform

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
        return rst.create_stac_item(
            source=str(tiff_path),
            collection=self.collection_id,
            collection_url=str(self.collection_file),
        )


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


def command_list_input_files(
    glob: str,
    input_dir: Path,
):
    """Build a STAC collection from a directory of geotiff files."""
    breakpoint()
    # builder: STACBuilder = _setup_builder(
    #     input_dir=Path(input_dir).expanduser().absolute(),
    #     glob=glob,
    #     overwrite=True,
    # )

    builder = STACBuilder()
    builder.glob = glob
    builder.input_dir = Path(input_dir).expanduser().absolute()

    builder.collect_input_files()

    for f in builder.input_files:
        print(f)


def command_list_metadata(
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
        df = builder.metadata_as_dataframe()
        print(df.to_string())
    else:
        metadata: Metadata
        for metadata in builder.try_parse_metadata():
            pprint.pprint(metadata.to_dict(include_internal=True))
            print()


def command_list_stac_items(
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
        print(df.to_string())
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
    """Validate a STAC collection."""
    builder = STACBuilder()
    collection_config_path = Path(collection_config_path)
    conf_contents = collection_config_path.read_text()
    config = CollectionConfig(**json.loads(conf_contents))
    builder.collection_config = config

    out_dir = Path(output_dir) if output_dir else None
    builder.post_process_collection(Path(collection_file), out_dir)
