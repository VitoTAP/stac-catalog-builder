"""The core module of the STAC catalog builder.

This contains the classes that generate the STAC catalogs, collections and items.
"""

# Standard libraries
import datetime as dt
from http.client import RemoteDisconnected
import inspect
import json
import logging
import shutil
import tempfile
from functools import partial
from pathlib import Path
from typing import Any, Callable, Dict, Hashable, Iterable, List, Optional, Tuple, Union


# Third party libraries
import geopandas as gpd
import pandas as pd
from pystac import Asset, CatalogType, Collection, Extent, Item, SpatialExtent, TemporalExtent
from pystac.errors import STACValidationError
from pystac.layout import TemplateLayoutStrategy

# TODO: add the GridExtension support again
from pystac.extensions.item_assets import AssetDefinition, ItemAssetsExtension
from pystac.extensions.projection import ItemProjectionExtension
from pystac.extensions.file import FileExtension
from pystac.extensions.eo import EOExtension, Band as EOBand
from pystac.extensions.raster import RasterExtension, RasterBand

# TODO: add datacube extension: https://github.com/VitoTAP/stac-catalog-builder/issues/19

# Modules from this project
from stacbuilder.exceptions import InvalidOperation, InvalidConfiguration
from stacbuilder.config import (
    AssetConfig,
    AlternateHrefConfig,
    CollectionConfig,
    FileCollectorConfig,
)
from stacbuilder.metadata import AssetMetadata, GeodataframeExporter
from stacbuilder.collector import GeoTiffMetadataCollector, IMetadataCollector


CLASSIFICATION_SCHEMA = "https://stac-extensions.github.io/classification/v1.0.0/schema.json"
ALTERNATE_ASSETS_SCHEMA = "https://stac-extensions.github.io/alternate-assets/v1.1.0/schema.json"


_logger = logging.getLogger(__name__)


class AlternateHrefGenerator:
    """Generates the alternate links for assets."""

    # Type alias for the specific callable that AlternateHrefGenerator needs.
    AssetMetadataToURL = Callable[[AssetMetadata], str]

    def __init__(self):
        self._callbacks: Dict[str, self.AssetMetadataToURL] = {}

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
        self.register_callback("MEP", lambda asset_md: asset_md.asset_path.as_posix())

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
        path = cls.remove_leading_trailing_slash(asset_md.asset_path.as_posix())
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


class MapMetadataToSTACItem:
    """Converts AssetMetadata objects to STAC Items.

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
        """Create the alternate links."""
        if not self._alternate_href_generator:
            return None

        if not metadata.asset_path:
            return None

        return self._alternate_href_generator.get_alternates(metadata)

    def create_item(self, assets: List[AssetMetadata]) -> Item:
        def is_known_asset_type(metadata: AssetMetadata) -> bool:
            return metadata.asset_type in self.item_assets_configs

        known_assets = [a for a in assets if is_known_asset_type(a)]
        if not known_assets:
            error_msg = "None of the assets in 'assets' is a known item type, not defined in collection configuration."
            _logger.warning(error_msg)
            return None

        # Ensure that the asset are all for the same STAC item
        assert len(set(a.item_id for a in assets)) == 1
        assert len(set(a.item_href for a in assets)) == 1

        first_asset = known_assets[0]
        item = Item(
            href=first_asset.item_href,
            id=first_asset.item_id,
            geometry=first_asset.geometry_as_dict,
            bbox=first_asset.bbox_as_list,
            datetime=first_asset.datetime,
            start_datetime=first_asset.start_datetime,
            end_datetime=first_asset.end_datetime,
            properties={
                "product_version": first_asset.version,
                "product_tile": first_asset.tile_id,
            },
        )

        # TODO: looks like we should get description from a source/config at the item level.
        description = self.item_assets_configs[first_asset.asset_type].description
        item.common_metadata.description = description

        item.common_metadata.created = dt.datetime.utcnow()

        # TODO: support summaries: these fields are recommended but they are also not always relevant or present.
        #   Originally defined in a module with only constants but now we work with configuration
        #   or extracting it from the source.
        #   This is part of https://github.com/VitoTAP/stac-catalog-builder/issues/18
        # item.common_metadata.mission = constants.MISSION
        # item.common_metadata.platform = constants.PLATFORM
        # item.common_metadata.instruments = constants.INSTRUMENTS

        def to_tuple_or_none(value) -> Union[Tuple, None]:
            if value is None:
                return None
            return tuple(value)

        #
        # Do some sanity checks on the asset metadata.
        # There can be multiple assets in a STAC item and we want them to be consistent.
        # For example:
        # It is not really possible to say what the CRS of the STAC item is when the assets have a mix of different CRSs.
        #
        # All assets should have the same CRS
        assert len(set(a.proj_epsg for a in assets)) == 1, "All assets should have the same CRS"
        # To be on the safe side also check the that the corresponding projection transform
        # is the same for all assets.
        assert (
            len(set(to_tuple_or_none(a.transform) for a in assets)) == 1
        ), "All assets should have the same projection transform"

        # All assets should have the same bounding box
        assert (
            len(set(to_tuple_or_none(a.bbox_as_list) for a in assets)) == 1
        ), "All assets should have the same lat-lon bounding box"
        assert (
            len(set(to_tuple_or_none(a.proj_bbox_as_list) for a in assets)) == 1
        ), "All assets should have the same projected bounding box"

        # All assets should also have the same shape (width and height in pixels)
        assert len(set(to_tuple_or_none(a.shape) for a in assets)) == 1, "All assets should have the same shape"

        for metadata in assets:
            item.add_asset(metadata.asset_type, self._create_asset(metadata, item))

        item_proj = ItemProjectionExtension.ext(item, add_if_missing=True)
        if metadata.proj_epsg:
            item_proj.epsg = first_asset.proj_epsg
        item_proj.bbox = first_asset.proj_bbox_as_list
        item_proj.geometry = first_asset.proj_geometry_as_dict
        item_proj.transform = first_asset.transform
        item_proj.shape = first_asset.shape

        # TODO: support optional parts: grid extension is recommended if we are indeed on a grid, but
        #    that is not always the case.
        #
        # The tile ID is not always the format that GridExtension expects.
        #   We would need a way to customize extracting that tile ID for the specific dataset.
        # TODO: investigate when/when not to include the GridExtension.
        #
        # if metadata.tile_id:
        #     grid = GridExtension.ext(item, add_if_missing=True)
        #     grid.code = metadata.tile_id

        asset_config: AssetConfig = self._get_assets_config_for(metadata.asset_type)
        if asset_config.eo_bands:
            EOExtension.add_to(item)
            item_eo = EOExtension.ext(item, add_if_missing=True)
            eo_bands = []

            # TODO: detect if the band is an Electro-Optical one.
            # TODO: https://github.com/VitoTAP/stac-catalog-builder/issues/29 set band's common_name property if common band, and wavelenght info when available
            for band_cfg in asset_config.eo_bands:
                new_band: EOBand = EOBand.create(
                    name=band_cfg.name,
                    description=band_cfg.description,
                )
                eo_bands.append(new_band)
            item_eo.apply(eo_bands)

        item.stac_extensions.append(CLASSIFICATION_SCHEMA)
        item.stac_extensions.append(ALTERNATE_ASSETS_SCHEMA)

        return item

    def _create_asset(self, metadata: AssetMetadata, item: Item) -> Asset:
        asset_defs = self._get_assets_definitions()
        asset_def: AssetDefinition = asset_defs[metadata.asset_type]
        asset_config = self._get_assets_config_for(metadata.asset_type)
        asset: Asset = asset_def.create_asset(metadata.href)
        asset.set_owner(item)
        asset.media_type = metadata.media_type

        if metadata.file_size:
            file_info = FileExtension.ext(asset, add_if_missing=True)
            file_info.size = metadata.file_size

        if metadata.raster_metadata:
            asset_raster = RasterExtension.ext(asset, add_if_missing=True)
            raster_bands = []

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

                    raster_bands.append(new_band)

            asset_raster.apply(raster_bands)

        # Add the alternate links for the Alternate-Asset extension
        # see: https://github.com/stac-extensions/alternate-assets
        if metadata.asset_path:
            alternate_links = self.create_alternate_links(metadata)
            if alternate_links:
                asset.extra_fields.update(alternate_links)

        return asset

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


class STACCollectionBuilder:
    """Creates a STAC Collection from STAC Items."""

    def __init__(
        self,
        collection_config: CollectionConfig,
        output_dir: Path,
        overwrite: bool = False,
        link_items: Optional[bool] = True,
    ) -> None:
        # Settings: these are just data, not components we delegate work to.
        self._collection_config = collection_config

        if not output_dir:
            raise ValueError(
                'Value for "output_dir" must be a Path instance. It can not be None or the empty string.'
                + f"{output_dir=!r}"
            )
        self._output_dir = Path(output_dir)

        self._overwrite_output = bool(overwrite)
        self._link_items = bool(link_items)

        # The result
        self._collection: Collection = None
        self._stac_items: List[Item] = None

    def reset(self):
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
    def link_items(self) -> bool:
        return self._link_items

    @link_items.setter
    def link_items(self, value: bool) -> bool:
        self._link_items = bool(value)

    @property
    def item_assets_configs(self) -> Dict[str, AssetConfig]:
        return self.collection_config.item_assets or {}

    @property
    def collection_file(self) -> Path:
        return self.output_dir / "collection.json"

    @property
    def collection(self) -> Optional[Collection]:
        return self._collection

    def build_collection(
        self,
        stac_items: Iterable[Item],
        group: Optional[str | int] = None,
    ) -> None:
        """Create and save the STAC collection."""
        self.reset()
        self._stac_items = list(stac_items) or []
        self.create_empty_collection(group=group)
        if self.link_items:
            self.add_items_to_collection()

    def add_items_to_collection(
        self,
    ):
        """Fills the collection with stac items."""
        self._log_progress_message("START: add_items_to_collection")

        if self._collection is None:
            raise InvalidOperation("Can not add items to a collection that has not been created yet.")

        item: Item
        for item in self._stac_items:
            # for asset in item.assets:
            #     asset.owner = self._collection
            if item is None:
                continue
            self._collection.add_item(item)

        self._log_progress_message("updating collection extent")
        self._collection.update_extent_from_items()

        self._log_progress_message("DONE: add_items_to_collection")

    def save_items_outside_collection(
        self,
    ):
        """Fills the collection with stac items."""
        self._log_progress_message("START: save_items_outside_collection")

        if self._collection is None:
            raise InvalidOperation("Can not add items to a collection that has not been created yet.")

        items = [i for i in self._stac_items if i is not None]
        item: Item
        for item in items:
            item.collection = self._collection

        stac_item_dir = self.stac_item_dir()
        if not stac_item_dir.exists():
            stac_item_dir.mkdir(parents=True)

        num_items = len(items)
        for i, item in enumerate(items):
            if i % 1000 == 0:
                fraction_done = i / num_items
                self._log_progress_message(f"Saved {i} of {num_items} STAC items. ({fraction_done:.1%})")
            # item.validate()
            item_path = self.get_item_path(item)
            if not item_path.parent.exists():
                item_path.parent.mkdir(parents=True)
            item.save_object(dest_href=item_path.as_posix(), include_self_link=False)

        self._log_progress_message("updating collection extent")
        self._collection.extent = Extent.from_items(items)

        self._log_progress_message("DONE: save_items_outside_collection")

    def stac_item_dir(self) -> Path:
        return self.output_dir / self.collection.id

    def get_item_path(self, item: Item) -> Path:
        year = f"{item.datetime.year:04}"
        month = f"{item.datetime.month:02}"
        day = f"{item.datetime.day:02}"
        return self.stac_item_dir() / year / month / day / f"{item.id}.json"

    def normalize_hrefs(self, skip_unresolved: bool = False):
        layout_template = self._collection_config.layout_strategy_item_template
        strategy = TemplateLayoutStrategy(item_template=layout_template)

        out_dir_str = self.output_dir.as_posix()
        if out_dir_str.endswith("/"):
            out_dir_str = out_dir_str[-1]
        self._collection.normalize_hrefs(root_href=out_dir_str, strategy=strategy, skip_unresolved=skip_unresolved)

    def validate_collection(self, collection: Collection):
        """Run STAC validation on the collection."""
        self._log_progress_message("START: validate_collection")
        try:
            num_items_validated = collection.validate_all(recursive=True)
        except STACValidationError as exc:
            print(exc)
            raise
        except RemoteDisconnected:
            print("Skipped this step validation due to RemoteDisconnected.")
        else:
            print(f"Collection valid: number of items validated: {num_items_validated}")

        self._log_progress_message("DONE: validate_collection")

    def save_collection(self) -> None:
        """Save the STAC collection to file."""
        self._log_progress_message("START: Saving collection ...")

        if not self.output_dir.exists():
            self.output_dir.mkdir(parents=True)

        # NOTE: creating a self-contained collection allows to move the collection and item files
        # but this is not enough to also be able to move the assets.
        # The href links to asset files also have the be relative (to the location of the STAC item)
        # This needs to be done via the href_modifier
        self._collection.save(catalog_type=CatalogType.SELF_CONTAINED)
        if not self.link_items:
            self.save_items_outside_collection()
        self._log_progress_message("DONE: Saving collection.")

    @property
    def providers(self):
        return [p.to_provider() for p in self._collection_config.providers]

    def create_empty_collection(self, group: Optional[str | int] = None) -> None:
        """Creates a STAC Collection with no STAC items."""
        self._log_progress_message("START: create_empty_collection")

        coll_config: CollectionConfig = self._collection_config

        if group:
            id = coll_config.collection_id + f"_{group}"
            title = coll_config.title + f" {group}"
        else:
            id = coll_config.collection_id
            title = coll_config.title

        collection = Collection(
            id=id,
            title=title,
            description=coll_config.description,
            keywords=coll_config.keywords,
            providers=self.providers,
            extent=self.get_default_extent(),
            # summaries=constants.SUMMARIES,
        )
        # TODO: Add support for summaries: https://github.com/VitoTAP/stac-catalog-builder/issues/18
        #   Summaries should, among other things, contain the platforms, instruments and mission, when available.
        #   In STAC these are singular but in fact there can be multiple.
        #   If there are multiple values we encode it as a string containing comma-separated values.

        item_assets_ext = ItemAssetsExtension.ext(collection, add_if_missing=True)
        item_assets_ext.item_assets = self._get_item_assets_definitions()

        RasterExtension.add_to(collection)
        collection.stac_extensions.append(CLASSIFICATION_SCHEMA)
        # TODO add the eo:bands extension:

        # TODO: Add support for custom links in the collection, like there was in the early scripts.
        ## collection.add_links(
        ##     [
        ##         constants.PRODUCT_FACT_SHEET,
        ##         constants.PROJECT_WEBSITE,
        ##     ]
        ## )

        self._collection = collection
        self._log_progress_message("DONE: create_empty_collection")

    def get_default_extent(self) -> Extent:
        end_dt = dt.datetime.utcnow()

        return Extent(
            # Default spatial extent is the entire world.
            SpatialExtent([-180.0, -90.0, 180.0, 90.0]),
            # Default temporal extent is from 1 year ago up until now.
            TemporalExtent(
                [
                    [
                        end_dt - dt.timedelta(weeks=52),
                        end_dt,
                    ]
                ]
            ),
        )

    def _get_item_assets_definitions(self) -> List[AssetDefinition]:
        asset_definitions = {}
        asset_configs = self._collection_config.item_assets

        for band_name, asset_config in asset_configs.items():
            asset_def: AssetDefinition = asset_config.to_asset_definition()
            # TODO: check whether we do need to store the collection or the items as the asset owner here.
            #   see also pystac docs: https://pystac.readthedocs.io/en/stable/api/pystac.html#pystac.Asset.owner
            #   Looks like there are two situations: item_assets occurs both at the level of the STAC collection and
            #   at the level of the STAC item, so the owner should be set accordingly.
            asset_def.owner = self.collection
            asset_definitions[band_name] = asset_def

        return asset_definitions

    def _log_progress_message(self, message: str) -> None:
        calling_method_name = inspect.stack()[1][3]
        _logger.info(f"PROGRESS: {self.__class__.__name__}.{calling_method_name}: {message}")


class PostProcessSTACCollectionFile:
    """Takes an existing STAC collection file and runs optional postprocessing steps.

    Sometimes there are situations where we need to fix small things quickly, and the easiest way
    is to do some post processing. For example overriding proj:bbox in at the collection level.
    This is what the PostProcessSTACCollectionFile is for.

    Most of this could be done by hand, but making it part of the pipeline and the configuration file
    makes it reproducible, and easy to repeat.


    Until recently processing included 2 steps, but now it has been reduced to one step, applying some simple overrides.

    These are specific key-value pairs in the JSON file, that we just fill in or overwrite
    with fixed values, after generating the collection file.
    These keys and values are read from the collection config file or CollectionConfig object.
    """

    def __init__(self, collection_overrides: Optional[Dict[str, Any]]) -> None:
        # Settings
        self._collection_overrides = collection_overrides or {}

    @property
    def collection_overrides(self) -> Optional[Dict[str, Any]]:
        return self._collection_overrides

    def process_collection(self, collection_file: Path, output_dir: Optional[Path] = None):
        process_in_place = self.is_in_place_processing(collection_file, output_dir)

        if not self.collection_overrides:
            _logger.info(
                "There is nothing to postprocess because no collection overrides are specified in "
                + "self.collection_overrides."
            )
            # If this is postprocessing is performed in-place on the collection file then we don't need to apply any changes.
            # But if an output_dir was specified we still need to copy the files to the new directory.
            # (Unless output_dir actually points to where the collection is located)
            if process_in_place:
                # Nothing left to do.
                return

        if not process_in_place:
            self._create_post_proc_directory_structure(collection_file, output_dir, copy_files=True)

        new_coll_file = self.get_converted_collection_path(collection_file, output_dir)
        data = self._load_collection_as_dict(new_coll_file)
        self._override_collection_components(data)
        self._save_collection_as_dict(data, new_coll_file)

    def is_in_place_processing(self, collection_file: Path, output_dir: Path) -> bool:
        return not output_dir or (output_dir.exists() and collection_file.parent.samefile(output_dir))

    def get_converted_collection_path(self, collection_file, output_dir) -> Path:
        if self.is_in_place_processing(collection_file, output_dir):
            return collection_file
        else:
            return output_dir / collection_file.name

    def get_converted_item_paths(self, collection_file: Path, output_dir: Path):
        item_paths = self.get_item_paths_for_coll_file(collection_file)
        if self.is_in_place_processing(collection_file, output_dir):
            return item_paths

        relative_paths = [ip.relative_to(collection_file.parent) for ip in item_paths]
        return [output_dir / rp for rp in relative_paths]

    def get_item_paths_for_collection(self, collection: Collection) -> List[Path]:
        items = collection.get_all_items()
        return [Path(item.self_href) for item in items]

    def get_item_paths_for_coll_file(self, collection_file: Path) -> List[Path]:
        collection = Collection.from_file(collection_file)
        return self.get_item_paths_for_collection(collection)

    def _create_post_proc_directory_structure(
        self, collection_file: Path, output_dir: Optional[Path] = None, copy_files: bool = False
    ):
        if self.is_in_place_processing(collection_file, output_dir):
            raise InvalidOperation(
                "Can not create identical directory structure when post-processing is executed in place."
                + f"{collection_file=}, {output_dir=}"
            )

        # Overwriting => remove and re-create the old directory
        if output_dir.exists():
            shutil.rmtree(output_dir)

        # Replicate the entire directory structure, so also the subfolders where items are grouped,
        # as specified by the layout_strategy_item_template in CollectionConfig
        output_dir.mkdir(parents=True)
        new_item_paths = self.get_converted_item_paths(collection_file, output_dir)

        sub_directories = set(p.parent for p in new_item_paths)
        for sub_dir in sub_directories:
            if not sub_dir.exists():
                sub_dir.mkdir(parents=True)

        # Also copy the relevant files if requested.
        if copy_files:
            collection_converted_file = self.get_converted_collection_path(collection_file, output_dir)
            shutil.copy2(collection_file, collection_converted_file)
            item_paths = self.get_item_paths_for_coll_file(collection_file)
            for old_path, new_path in zip(item_paths, new_item_paths):
                shutil.copy2(old_path, new_path)

    def _override_collection_components(self, data: Dict[str, Any]) -> None:
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

    @staticmethod
    def _load_collection_as_dict(coll_file: Path) -> dict:
        with open(coll_file, "r") as f_in:
            return json.load(f_in)

    @staticmethod
    def _save_collection_as_dict(data: Dict[str, Any], coll_file: Path) -> None:
        with open(coll_file, "w") as f_out:
            json.dump(data, f_out, indent=2)


class AssetMetadataPipeline:
    """Converts AssetMetadata to STAC collections."""

    def __init__(
        self,
        metadata_collector: IMetadataCollector,
        output_dir: Path,
        overwrite: Optional[bool] = False,
        link_items: Optional[bool] = True,
    ) -> None:
        # Settings: these are just data, not components we delegate work to.
        self._output_base_dir: Path = self._get_output_dir_or_default(output_dir)
        self._collection_dir: Path = None
        self._overwrite: bool = bool(overwrite)
        self._link_items = bool(link_items)

        # Components / dependencies that must be provided
        self._metadata_collector: IMetadataCollector = metadata_collector

        # Components / dependencies that we set up internally
        self._meta_to_stac_item_mapper: MapMetadataToSTACItem = None
        self._func_find_item_group: Optional[Callable[[Item], str]] = None

        self._collection_builder: STACCollectionBuilder = None

        self._item_postprocessor: Optional[Callable] = None

        # results
        self._collection: Optional[Collection] = None
        self._collection_groups: Dict[Hashable, Collection] = {}

    @staticmethod
    def from_config(
        metadata_collector: IMetadataCollector,
        collection_config: CollectionConfig,
        output_dir: Optional[Path] = None,
        overwrite: Optional[bool] = False,
        link_items: Optional[bool] = True,
    ) -> "AssetMetadataPipeline":
        """Creates a AssetMetadataPipeline from configurations."""

        pipeline = AssetMetadataPipeline(metadata_collector=None, output_dir=None, overwrite=False)
        pipeline._setup(
            metadata_collector=metadata_collector,
            collection_config=collection_config,
            output_dir=output_dir,
            overwrite=overwrite,
            link_items=link_items,
        )
        return pipeline

    def _setup(
        self,
        metadata_collector: IMetadataCollector,
        collection_config: CollectionConfig,
        output_dir: Optional[Path] = None,
        overwrite: Optional[bool] = False,
        link_items: Optional[bool] = True,
    ) -> None:
        """Set up an existing instance using the specified dependencies and configuration settings."""

        if metadata_collector is None:
            raise ValueError(
                'Argument "metadata_collector" can not be None, must be a IMetadataCollector implementation.'
            )

        if collection_config is None:
            raise ValueError('Argument "collection_config" can not be None, must be a CollectionConfig instance.')

        # Dependencies or components that we delegate work to.
        self._metadata_collector = metadata_collector

        # Settings: these are just data, not components we delegate work to.
        self._collection_config = collection_config
        self._output_base_dir = self._get_output_dir_or_default(output_dir)
        self._overwrite = overwrite
        self._link_items = bool(link_items)

        self._setup_internals()

    @staticmethod
    def _get_output_dir_or_default(output_dir: Path | str | None) -> Path:
        return Path(output_dir) if output_dir else Path(tempfile.gettempdir())

    def _setup_internals(
        self,
        group: str | int | None = None,
    ) -> None:
        """Setup the internal components based on the components that we receive via dependency injection."""

        self._meta_to_stac_item_mapper = MapMetadataToSTACItem(item_assets_configs=self.item_assets_configs)

        # The default way we group items into multiple collections is by year
        # Currently we don't use any other ways to create a group of collections.
        self._func_find_item_group = lambda item: item.datetime.year

        if group and not self.uses_collection_groups:
            raise InvalidOperation("You can only use collection groups when the pipeline is configured for grouping.")

        if group:
            self._collection_dir = self.get_collection_file_for_group(group)
        else:
            self._collection_dir = self._output_base_dir

        self._collection_builder = STACCollectionBuilder(
            collection_config=self._collection_config,
            overwrite=self._overwrite,
            output_dir=self._collection_dir,
            link_items=self._link_items,
        )

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
    def meta_to_stac_item_mapper(self) -> MapMetadataToSTACItem:
        return self._meta_to_stac_item_mapper

    @property
    def item_postprocessor(self) -> Optional[Callable]:
        return self._item_postprocessor

    @item_postprocessor.setter
    def item_postprocessor(self, callable: Callable) -> None:
        self._item_postprocessor = callable

    def reset(self) -> None:
        self._collection = None
        self._collection_groups = {}

    def get_metadata(self) -> Iterable[AssetMetadata]:
        """Generate the intermediate metadata objects, from the input files."""
        self._metadata_collector.collect()
        return self._metadata_collector.metadata_list

    @property
    def uses_collection_groups(self):
        return self._func_find_item_group is not None

    def group_stac_items_by(self) -> Dict[int, List[Item]]:
        groups: Dict[int, AssetMetadata] = {}
        iter_items = self.collect_stac_items()

        for item in iter_items:
            group = self._func_find_item_group(item)

            if group not in groups:
                groups[group] = []

            groups[group].append(item)

        return groups

    def collect_stac_items(self):
        """Generate the intermediate STAC Item objects."""
        self._log_progress_message("START: collect_stac_items")

        groups = self.group_metadata_by_item_id(self.get_metadata())
        num_groups = len(groups)

        progress_chunk_size = 100
        for i, assets in enumerate(groups.values()):
            if i % progress_chunk_size == 0:
                fraction_done = i / num_groups
                self._log_progress_message(
                    f"Converted {i} of {num_groups} AssetMetadata to STAC Items ({fraction_done:.1%})"
                )

            stac_item = self._meta_to_stac_item_mapper.create_item(assets)
            # Ignore the asset when the file was not a known asset type, for example it is
            # not a GeoTIFF or it is not one of the assets or bands we want to include.
            if stac_item:
                if self._item_postprocessor is not None:
                    stac_item = self._item_postprocessor(stac_item)
                # try:
                #     stac_item.validate()
                # except RemoteDisconnected:
                #     print(f"Skipped validation of {stac_item.get_self_href()} due to RemoteDisconnected.")
                yield stac_item

        self._log_progress_message("DONE: collect_stac_items")

    def group_metadata_by_item_id(self, iter_metadata) -> Dict[int, List[Item]]:
        self._log_progress_message("START: group_metadata_by_item_id")
        groups: Dict[int, AssetMetadata] = {}

        for metadata in iter_metadata:
            item_id = metadata.item_id

            if item_id not in groups:
                groups[item_id] = []

            groups[item_id].append(metadata)

        self._log_progress_message("DONE: group_metadata_by_item_id")
        return groups

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

    def build_collection(
        self,
    ):
        """Build the entire STAC collection."""
        self._log_progress_message("START: build_collection")

        self.reset()

        self._collection_builder.build_collection(self.collect_stac_items())
        self._collection_builder.normalize_hrefs()
        self._collection_builder.save_collection()
        self._collection = self._collection_builder.collection

        coll_file = self._collection_builder.collection_file
        post_processor = PostProcessSTACCollectionFile(collection_overrides=self._collection_config.overrides)
        post_processor.process_collection(coll_file)

        self._log_progress_message("DONE: build_collection")

    def get_collection_file_for_group(self, group: str | int):
        return self._output_base_dir / str(group)

    def build_grouped_collections(self):
        self._log_progress_message("START: build_grouped_collections")

        self.reset()

        if not self.uses_collection_groups:
            raise InvalidOperation(f"This instance of {self.__class__.__name__} does not have grouping.")

        self._root_collection_builder = STACCollectionBuilder(
            collection_config=self._collection_config,
            overwrite=self._overwrite,
            output_dir=self._output_base_dir,
        )
        self._root_collection_builder.create_empty_collection()

        for group, metadata_list in sorted(self.group_stac_items_by().items()):
            self._setup_internals(group=group)

            self._collection_builder.build_collection(stac_items=metadata_list, group=group)
            self._root_collection_builder.collection.add_child(self._collection_builder.collection)
            self._collection_groups[group] = self._collection_builder.collection

        self._root_collection_builder.normalize_hrefs()
        self._root_collection_builder.collection.update_extent_from_items()
        self._root_collection_builder.save_collection()

        # post process
        post_processor = PostProcessSTACCollectionFile(collection_overrides=self._collection_config.overrides)
        post_processor.process_collection(self._root_collection_builder.collection_file)
        for group in self._collection_groups.keys():
            coll_file = Path(self._collection_groups[group].self_href)
            post_processor.process_collection(coll_file)

        self._log_progress_message("DONE: build_grouped_collections")

    def _log_progress_message(self, message: str) -> None:
        calling_method_name = inspect.stack()[1][3]
        _logger.info(f"PROGRESS: {self.__class__.__name__}.{calling_method_name}: {message}")


class GeoTiffPipeline:
    """A pipeline to generate a STAC collection from a directory containing GeoTIFF files.
    TODO: move remaining logic (from_config) to commandapi and remove this class. Follow example of command `vpp_build_collection`
    """

    def __init__(
        self,
        metadata_collector: GeoTiffMetadataCollector,
        asset_metadata_pipeline: AssetMetadataPipeline,
    ) -> None:
        # Components / dependencies that must be provided
        if not metadata_collector:
            raise ValueError("You must provide an IMetadataCollector implementation for metadata_collector")

        if not asset_metadata_pipeline:
            raise ValueError("You must provide an AssetMetadataPipeline instance for asset_metadata_pipeline")

        self._metadata_collector: GeoTiffMetadataCollector = metadata_collector
        self._asset_metadata_pipeline: AssetMetadataPipeline = asset_metadata_pipeline

    @property
    def collection(self) -> Collection | None:
        return self._asset_metadata_pipeline.collection

    @property
    def collection_file(self) -> Path | None:
        return self._asset_metadata_pipeline.collection_file

    @property
    def collection_groups(self) -> Dict[Hashable, Collection] | None:
        return self._asset_metadata_pipeline.collection_groups

    @property
    def collection_config(self) -> CollectionConfig:
        return self._asset_metadata_pipeline.collection_config

    @property
    def metadata_collector(self) -> GeoTiffMetadataCollector:
        return self._metadata_collector

    @property
    def asset_metadata_pipeline(self) -> AssetMetadataPipeline:
        return self._asset_metadata_pipeline

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

        # TODO: this setup can be done in commandapi.py, analogous to the command vpp_build_collection.
        """
        if output_dir and not isinstance(output_dir, Path):
            raise TypeError(f"Argument output_dir (if not None) should be of type Path, {type(output_dir)=}")

        if collection_config is None:
            raise ValueError('Argument "collection_config" can not be None, must be a CollectionConfig instance.')

        if file_coll_cfg is None:
            raise ValueError('Argument "file_coll_cfg" can not be None, must be a FileCollectorConfig instance.')

        collection_config = collection_config
        metadata_collector = GeoTiffMetadataCollector.from_config(
            collection_config=collection_config,
            file_coll_cfg=file_coll_cfg,
        )
        asset_metadata_pipeline = AssetMetadataPipeline.from_config(
            metadata_collector=metadata_collector,
            collection_config=collection_config,
            output_dir=output_dir,
            overwrite=overwrite,
        )
        return GeoTiffPipeline(
            metadata_collector=metadata_collector,
            asset_metadata_pipeline=asset_metadata_pipeline,
        )

    def reset(self) -> None:
        self._collection = None
        self._collection_groups = {}
        self._metadata_collector.reset()
        self._asset_metadata_pipeline.reset()

    def get_input_files(self) -> Iterable[Path]:
        """Collect the input files for processing."""
        return self._metadata_collector.get_input_files()

    def get_asset_metadata(self) -> Iterable[AssetMetadata]:
        """Generate the intermediate asset metadata objects, from the input files."""
        self._metadata_collector.collect()
        return self._metadata_collector.metadata_list

    @property
    def uses_collection_groups(self):
        return self._asset_metadata_pipeline.uses_collection_groups is not None

    def collect_stac_items(self):
        """Generate the intermediate STAC Item objects."""
        return self._asset_metadata_pipeline.collect_stac_items()

    def get_metadata_as_geodataframe(self) -> gpd.GeoDataFrame:
        """Return a GeoDataFrame representing the intermediate metadata."""
        return self._asset_metadata_pipeline.get_metadata_as_geodataframe()

    def get_metadata_as_dataframe(self) -> pd.DataFrame:
        """Return a pandas DataFrame representing the intermediate metadata, without the geometry."""
        return self._asset_metadata_pipeline.get_metadata_as_dataframe()

    def get_stac_items_as_geodataframe(self) -> gpd.GeoDataFrame:
        """Return a GeoDataFrame representing the STAC Items."""
        return self._asset_metadata_pipeline.get_stac_items_as_geodataframe()

    def get_stac_items_as_dataframe(self) -> pd.DataFrame:
        """Return a pandas DataFrame representing the STAC Items, without the geometry."""
        return self._asset_metadata_pipeline.get_stac_items_as_dataframe()

    def build_collection(self):
        """Build the entire STAC collection."""
        self.reset()
        self._asset_metadata_pipeline.build_collection()

    def get_collection_file_for_group(self, group: str | int):
        return self._asset_metadata_pipeline.get_collection_file_for_group(group)

    def build_grouped_collections(self):
        self.reset()
        self._asset_metadata_pipeline.build_grouped_collections()
