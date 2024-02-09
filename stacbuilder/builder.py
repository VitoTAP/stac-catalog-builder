"""The core module of the STAC catalog builder.

This contains the classes that generate the STAC catalogs, collections and items.
"""

# Standard libraries
import datetime as dt
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
import rio_stac.stac as rst
from pystac import Asset, CatalogType, Collection, Extent, Item, SpatialExtent, TemporalExtent
from pystac.errors import STACValidationError
from pystac.layout import TemplateLayoutStrategy

# TODO: add the GridExtension support again
from pystac.extensions.item_assets import AssetDefinition, ItemAssetsExtension
from pystac.extensions.projection import ItemProjectionExtension
from pystac.extensions.file import FileExtension
from pystac.extensions.eo import EOExtension, Band as EOBand
from pystac.extensions.raster import RasterExtension, RasterBand

# TODO: add datacube extension


# our own libraries outside of this project
from stacbuilder.metadata import GeodataframeExporter

# Modules from this project
from stacbuilder.exceptions import InvalidOperation, InvalidConfiguration
from stacbuilder.config import (
    AssetConfig,
    AlternateHrefConfig,
    CollectionConfig,
    FileCollectorConfig,
)
from stacbuilder.metadata import AssetMetadata
from stacbuilder.timezoneformat import TimezoneFormatConverter
from stacbuilder.collector import GeoTiffMetadataCollector, IMetadataCollector, MapGeoTiffToAssetMetadata


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

    TODO: VVVV Is this function still needed?
    """
    return rst.create_stac_item(
        source=str(tiff_path),
        collection=collection_id,
        collection_url=str(collection_file),
    )


class MapMetadataToSTACItem:
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
            error_msg = (
                "None of the assets in 'assets' is a known item type, not defined in collection configuration: "
                + f"{assets}, returning item=None"
            )
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
                # TODO: what was product_tile in the original script and do we still need it. Else remove.
                # "product_tile": metadata.tile,
            },
        )
        # TODO: support optional parts: store the tile ID if dataset uses that.
        #   We would need a way to customize extracting that tile ID for the specific dataset.

        # TODO: looks like we should get description from a sourse/config at the item level.
        description = self.item_assets_configs[first_asset.asset_type].description
        item.common_metadata.description = description

        item.common_metadata.created = dt.datetime.utcnow()

        # TODO: support optional parts: these fields are recommended but they are also not always relevant or present.
        #   Originally defined in a module with only constants. Need to add this to the config classes.
        # item.common_metadata.mission = constants.MISSION
        # item.common_metadata.platform = constants.PLATFORM
        # item.common_metadata.instruments = constants.INSTRUMENTS

        def to_tuple_or_none(value) -> Union[Tuple, None]:
            if value is None:
                return None
            return tuple(value)

        assert len(set(a.proj_epsg for a in assets)) == 1
        assert len(set(to_tuple_or_none(a.bbox_as_list) for a in assets)) == 1
        assert len(set(to_tuple_or_none(a.proj_bbox_as_list) for a in assets)) == 1
        assert len(set(to_tuple_or_none(a.transform) for a in assets)) == 1
        assert len(set(to_tuple_or_none(a.shape) for a in assets)) == 1

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

        # TODO: Did original script do anything in code below that we still need to re-implement? And remove it after.
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


class STACCollectionBuilder:
    """Creates a STAC Collection from STAC Items."""

    def __init__(self, collection_config: CollectionConfig, output_dir: Path, overwrite: bool = False) -> None:
        # Settings: these are just data, not components we delegate work to.
        self._collection_config = collection_config

        if not output_dir:
            raise ValueError(
                'Value for "output_dir" must be a Path instance. It can not be None or the empty string.'
                + f"{output_dir=!r}"
            )
        self._output_dir = Path(output_dir)
        self._overwrite_output = overwrite

        # Internal temporary state
        self._stac_items: List[Item] = None

        # The result
        self._collection: Collection = None

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
            extent=self.get_default_extent(),
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

    def get_default_extent(self):
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


class AssetMetadataPipeline:
    """Converts AssetMetadata to STAC collections."""

    def __init__(
        self,
        metadata_collector: IMetadataCollector,
        output_dir: Path,
        overwrite: Optional[bool] = False,
    ) -> None:
        # Settings: these are just data, not components we delegate work to.
        self._output_base_dir: Path = self._get_output_dir_or_default(output_dir)
        self._collection_dir: Path = None
        self._overwrite: bool = overwrite

        # Components / dependencies that must be provided
        self._metadata_collector: IMetadataCollector = metadata_collector

        # Components / dependencies that we set up internally
        self._geotiff_to_metadata_mapper: MapGeoTiffToAssetMetadata = None
        self._meta_to_stac_item_mapper: MapMetadataToSTACItem = None
        # self._metadata_group_creator: IGroupMetadataBy = None
        self._func_find_item_group: Optional[Callable[[Item], str]] = None

        self._collection_builder: STACCollectionBuilder = None

        # results
        self._collection: Optional[Collection] = None
        self._collection_groups: Dict[Hashable, Collection] = {}

    @staticmethod
    def from_config(
        metadata_collector: IMetadataCollector,
        collection_config: CollectionConfig,
        output_dir: Optional[Path] = None,
        overwrite: Optional[bool] = False,
    ) -> "AssetMetadataPipeline":
        """Creates a AssetMetadataPipeline from configurations."""

        pipeline = AssetMetadataPipeline(metadata_collector=None, output_dir=None, overwrite=False)
        pipeline._setup(
            metadata_collector=metadata_collector,
            collection_config=collection_config,
            output_dir=output_dir,
            overwrite=overwrite,
        )
        return pipeline

    def _setup(
        self,
        metadata_collector: IMetadataCollector,
        collection_config: CollectionConfig,
        output_dir: Optional[Path] = None,
        overwrite: Optional[bool] = False,
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
        # self._metadata_group_creator = GroupMetadataByYear()
        self._func_find_item_group = lambda item: item.datetime.year

        # TODO: disabling support for collection groups until AssetMetadataPipeline works for regular collections first.
        if group and not self.uses_collection_groups:
            raise InvalidOperation("You can only use collection groups when the pipeline is configured for grouping.")

        if group:
            self._collection_dir = self.get_collection_file_for_group(group)
        else:
            self._collection_dir = self._output_base_dir

        self._collection_dir = self._output_base_dir
        self._collection_builder = STACCollectionBuilder(
            collection_config=self._collection_config,
            overwrite=self._overwrite,
            output_dir=self._collection_dir,
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
    def geotiff_to_metadata_mapper(self) -> MapGeoTiffToAssetMetadata:
        return self._geotiff_to_metadata_mapper

    @property
    def meta_to_stac_item_mapper(self) -> MapMetadataToSTACItem:
        return self._meta_to_stac_item_mapper

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

        groups = self.group_metadata_by_item_id(self.get_metadata())
        for assets in groups.values():
            stac_item = self._meta_to_stac_item_mapper.create_item(assets)
            # Ignore the asset when the file was not a known asset type, for example it is
            # not a GeoTIFF or it is not one of the assets or bands we want to include.
            if stac_item:
                stac_item.validate()
                yield stac_item

    # TODO: [simplify] [refactor] Merge this into collect_stac_items once it works well and it has tests.
    @staticmethod
    def group_metadata_by_item_id(iter_metadata) -> Dict[int, List[Item]]:
        groups: Dict[int, AssetMetadata] = {}

        for metadata in iter_metadata:
            item_id = metadata.item_id

            if item_id not in groups:
                groups[item_id] = []

            groups[item_id].append(metadata)

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

    def build_collection(self):
        """Build the entire STAC collection."""
        self.reset()

        self._collection_builder.build_collection(self.collect_stac_items())
        self._collection = self._collection_builder.collection

        coll_file = self._collection_builder.collection_file
        post_processor = PostProcessSTACCollectionFile(collection_overrides=self._collection_config.overrides)
        post_processor.process_collection(coll_file)

    # TODO: disabling support for collection groups until AssetMetadataPipeline works for regular collections first.
    def get_collection_file_for_group(self, group: str | int):
        return self._output_base_dir / str(group)

    def build_grouped_collections(self):
        self.reset()

        if not self.uses_collection_groups:
            raise InvalidOperation(f"This instance of {self.__class__.__name__} does not have grouping.")

        for group, metadata_list in sorted(self.group_stac_items_by().items()):
            self._setup_internals(group=group)

            self._collection_builder.build_collection(metadata_list)
            self._collection_groups[group] = self._collection_builder.collection

            coll_file = self._collection_builder.collection_file
            post_processor = PostProcessSTACCollectionFile(collection_overrides=self._collection_config.overrides)
            post_processor.process_collection(coll_file)


class GeoTiffPipeline:
    """A pipeline to generate a STAC collection from a directory containing GeoTIFF files."""

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
