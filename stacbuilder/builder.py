import datetime as dt
import json
import logging
import pprint

from itertools import islice
from pathlib import Path
from typing import Callable, Dict, List, Optional

import pandas as pd

from pystac import Asset, CatalogType, Collection, Extent, Item, SpatialExtent, TemporalExtent
from pystac.layout import TemplateLayoutStrategy

# from pystac.utils import make_absolute_href

from pystac.extensions.grid import GridExtension
from pystac.extensions.item_assets import AssetDefinition, ItemAssetsExtension
from pystac.extensions.projection import ItemProjectionExtension
from pystac.extensions.raster import RasterExtension


from stactools.core.io import ReadHrefModifier
import rio_stac.stac as rst


from stacbuilder.core import InputPathParser, InputPathParserFactory, NoopInputPathParser
from stacbuilder.config import AssetConfig, CollectionConfig
from stacbuilder.metadata import Metadata


_logger = logging.getLogger(__name__)


CLASSIFICATION_SCHEMA = "https://stac-extensions.github.io/classification/v1.0.0/schema.json"


class SettingsInvalid(Exception):
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

        self.output_dir: Path = None
        self.overwrite: bool = False

        self._path_parser: InputPathParser = path_parser
        self._read_href_modifier: Callable = None

        self._collection_config: CollectionConfig = None

        self._input_files: List[Path] = []
        self._collection: Collection = None

    def build_collection(self):
        self.validate_builder_settings()
        self.collect_input_files()
        self.create_collection()
        self.save_collection()
        self.validate_collection()

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

    def get_settings_errors(self):
        errors = []
        if not self.collection_config:
            errors.append(f"collection_config is not set")

        if not self.input_dir:
            errors.append(f"input_dir is not set")
        elif not self.input_dir.exists():
            errors.append(f"Input directory does not exist: {self.input_dir!r}")

        if not self.glob:
            errors.append(f'glob pattern is not set, default should be "*"')

        if not self.output_dir:
            errors.append(f"output_dir is not set")
        elif self.output_dir.exists() and not self.overwrite:
            errors.append(f"Output directory already exist but overwrite is OFF (False): {self.output_dir}")

        return errors

    def can_run(self) -> bool:
        return not self.get_settings_errors()

    def validate_builder_settings(self) -> None:
        errors = self.get_settings_errors()
        if errors:
            raise SettingsInvalid("\n".join(errors))

    def try_parse_metadata(self):
        extract_href_info = self._get_input_path_parser()
        for file in self.input_files:
            metadata = Metadata(
                href=str(file),
                extract_href_info=extract_href_info,
                read_href_modifier=self._read_href_modifier,
            )
            yield metadata

    def try_parse_items(self):
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
        self.validate_builder_settings()
        self._create_collection()

        for file in self.input_files:
            item = self.create_item(file)
            if item is not None:
                item.validate()
                self._collection.add_item(item)

        self._collection.update_extent_from_items()

        layout_template = self.collection_config.layout_strategy_item_template
        strategy = TemplateLayoutStrategy(item_template=layout_template)

        output_dir_str = str(self.output_dir)
        if output_dir_str.endswith("/"):
            output_dir_str = output_dir_str[-1]
        self._collection.normalize_hrefs(output_dir_str, strategy=strategy)

        return self._collection

    def validate_collection(self):
        return self._collection.validate_all(recursive=True)

    def save_collection(self):
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
        self._collection = collection

        item_assets_ext = ItemAssetsExtension.ext(collection, add_if_missing=True)
        item_assets_ext.item_assets = self.get_item_assets_definitions()

        RasterExtension.add_to(collection)
        collection.stac_extensions.append(CLASSIFICATION_SCHEMA)

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

    builder: STACBuilder = _setup_builder(
        input_dir=Path(input_dir).expanduser().absolute(),
        glob=glob,
        overwrite=True,
    )

    builder.collect_input_files()

    for f in builder.input_files:
        print(f)


def command_list_metadata(
    collection_config_path: Path,
    glob: str,
    input_dir: Path,
    max_files: Optional[int] = -1,
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

    metadata: Metadata
    for metadata in builder.try_parse_metadata():
        pprint.pprint(metadata.to_dict(include_internal=True))
        print()


def command_list_stac_items(
    collection_config_path: Path,
    glob: str,
    input_dir: Path,
    max_files: Optional[int] = -1,
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

    for item in builder.try_parse_items():
        pprint.pprint(item.to_dict())
        print()


def command_load_collection(
    path: Path,
):
    """Build a STAC collection from a directory of geotiff files."""

    builder = STACBuilder()
    builder.load_collection(path)
    pprint.pprint(builder.collection.to_dict(), indent=2)
