import datetime as dt
import json
import logging
import pprint

from pathlib import Path
from typing import Callable, Dict, List, Optional, Set


from pystac import Asset, CatalogType, Collection, Extent, Item, MediaType, SpatialExtent, TemporalExtent
from pystac.layout import TemplateLayoutStrategy
from pystac.utils import make_absolute_href, str_to_datetime

from pystac.extensions.grid import GridExtension
from pystac.extensions.item_assets import AssetDefinition, ItemAssetsExtension
from pystac.extensions.projection import ItemProjectionExtension
from pystac.extensions.raster import RasterExtension


from stactools.core.io import ReadHrefModifier
import rio_stac.stac as rst
import rasterio

# from pytac.provider import ProviderRole


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
        self.validate_builder_settings()
        input_files = [f for f in self.input_dir.glob(self.glob) if f.is_file()]

        if self.max_files_to_process > 0:
            input_files = input_files[: self.max_files_to_process]

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

    def try_parse_items(self):
        extract_href_info = self._get_input_path_parser()
        # all_item_metadata = []
        for file in self.input_files:
            metadata = Metadata(
                href=str(file),
                extract_href_info=extract_href_info,
                read_href_modifier=self._read_href_modifier,
            )
            yield metadata

            # all_item_metadata.append(metadata)

        # return all_item_metadata

    def create_collection(self):
        self.validate_builder_settings()

        collection = self._create_collection()

        for file in self.input_files:
            item = self.create_item(file)
            # item = self.get_item_from_rio_stac(file)
            collection.add_item(item)

        collection.update_extent_from_items()

        #
        # TODO: add layout strategy to collection config
        layout_template = self.collection_config.layout_strategy_item_template
        strategy = TemplateLayoutStrategy(item_template=layout_template)
        # strategy = TemplateLayoutStrategy(item_template="${collection}/${year}")
        ## strategy = TemplateLayoutStrategy(item_template="${collection}")

        output_dir_str = str(self.output_dir)
        if output_dir_str.endswith("/"):
            output_dir_str = output_dir_str[-1]
        collection.normalize_hrefs(output_dir_str, strategy=strategy)

        self._collection = collection
        return self._collection

    def validate_collection(self):
        return self._collection.validate_all(recursive=True)

    def save_collection(self):
        _logger.info("Saving files ...")
        self._collection.save(catalog_type=CatalogType.SELF_CONTAINED)

        # TODO: do we still need this check?
        print(f"{self.collection_file=}")
        assert Path(self.collection_file).exists()

        return self.collection_file

    def _create_collection(self) -> Collection:
        """Creates a STAC Collection."""

        # summaries = constants.SUMMARIES
        # summaries["eo:bands"] = ASSET_PROPS[self.band]["bands"]

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

        item_assets_ext = ItemAssetsExtension.ext(collection, add_if_missing=True)
        item_assets_ext.item_assets = self.get_item_assets_defs()

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
        return self.collection_config.item_assets

    def get_item_assets_defs(self) -> List[AssetDefinition]:
        asset_definitions = {}
        for band_name, asset_config in self.item_assets_configs.items():
            asset_definitions[band_name] = asset_config.to_asset_definition()

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

        if not metadata.item_id:
            print("metadata.item_id not set")
            breakpoint()

        item = Item(
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

        # item.common_metadata.description = constants.ITEM_DESCRIPTION
        item.common_metadata.description = self.collection_description

        # item.common_metadata.created = dt.datetime.now(tz=dt.timezone.utc)
        item.common_metadata.created = dt.datetime.utcnow()

        # item.common_metadata.mission = constants.MISSION
        # item.common_metadata.platform = constants.PLATFORM
        # item.common_metadata.instruments = constants.INSTRUMENTS

        item.add_asset(metadata.band, self.create_asset(tiff_path))

        item_proj = ItemProjectionExtension.ext(item, add_if_missing=True)
        item_proj.epsg = metadata.proj_epsg.to_epsg()
        item_proj.bbox = metadata.proj_bbox
        item_proj.geometry = metadata.proj_geometry

        # grid = GridExtension.ext(item, add_if_missing=True)
        # grid.code = f"TILE-{metadata.tile}"

        RasterExtension.add_to(item)
        item.stac_extensions.append(CLASSIFICATION_SCHEMA)

        item.validate()

        return item

    def create_asset(self, tiff_path) -> Asset:
        asset = Asset(href=make_absolute_href(tiff_path))
        # asset.roles = ASSET_PROPS[self.band]["roles"]
        # asset.title = ASSET_PROPS[self.band]["title"]
        # asset.description = ASSET_PROPS[self.band]["description"]

        # TODO: set the MediaType to use in the Metadata constructor
        asset.media_type = self.collection_config.media_type

        # extra_fields = {"eo:bands": ASSET_PROPS[self.band]["bands"]}
        # asset.extra_fields = extra_fields

        return asset

    def get_item_from_rio_stac(
        self,
        tiff_path: Path,
    ):
        return rst.create_stac_item(
            source=str(tiff_path),
            collection=self.collection_id,
            collection_url=str(self.collection_file),
        )

    # def collect_item_metadata(self, geotiff_path: Path):

    #     import rasterio
    #     with rasterio.open(geotiff_path) as dataset:
    #         proj_bbox = list(dataset.bounds)
    #         proj_epsg = dataset.crs
    #         transform = list(dataset.transform)[0:6]
    #         shape = dataset.shape
    #         tags = dataset.tags()
    #         print(dataset)


def _setup_builder(
    collection_config_path: Path,
    glob: str,
    input_dir: Path,
    output_dir: Path,
    overwrite: bool,
    max_files_to_process: Optional[int] = -1,
) -> STACBuilder:
    """Build a STAC collection from a directory of geotiff files."""

    builder = STACBuilder()

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


def command_gather_inputs(
    collection_config_path: Path,
    glob: str,
    input_dir: Path,
):
    """Build a STAC collection from a directory of geotiff files."""

    builder: STACBuilder = _setup_builder(
        collection_config_path=Path(collection_config_path).expanduser().absolute(),
        glob=glob,
        input_dir=Path(input_dir).expanduser().absolute(),
        output_dir=Path("/tmp"),
        overwrite=True,
    )

    builder.validate_builder_settings()
    builder.collect_input_files()

    # for f in builder.input_files:
    #     print(f)

    for metadata in builder.try_parse_items():
        pprint.pprint(metadata.to_dict())

    if builder.input_files:
        item = builder.get_item_from_rio_stac(builder.input_files[-1])
        item_dict = item.to_dict(include_self_link=False)
        pprint.pprint(item_dict, indent=2)


def command_load_collection(
    path: Path,
):
    """Build a STAC collection from a directory of geotiff files."""

    builder = STACBuilder()
    builder.load_collection(path)
    pprint.pprint(builder.collection.to_dict(), indent=2)
