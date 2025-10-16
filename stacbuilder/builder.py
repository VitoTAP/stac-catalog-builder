"""The core module of the STAC catalog builder.

This contains the classes that generate the STAC catalogs, collections and items.
"""

# Standard libraries
import datetime as dt
import gc
import inspect
import logging
from functools import partial
from http.client import RemoteDisconnected
from pathlib import Path
from typing import Callable, Dict, Generator, Hashable, Iterable, List, Optional, Tuple

# Third party libraries
import deprecated
from pyproj import CRS
from pystac import (
    Asset,
    CatalogType,
    Collection,
    Extent,
    Item,
    SpatialExtent,
    TemporalExtent,
)
from pystac.errors import STACValidationError
from pystac.extensions.datacube import (
    AdditionalDimension,
    DatacubeExtension,
    SpatialDimension,
    TemporalDimension,
)
from pystac.extensions.eo import EOExtension
from pystac.extensions.file import FileExtension
from pystac.extensions.item_assets import AssetDefinition, ItemAssetsExtension
from pystac.extensions.projection import ItemProjectionExtension
from pystac.extensions.raster import RasterBand, RasterExtension
from pystac.layout import TemplateLayoutStrategy

from stacbuilder.collector import IMetadataCollector
from stacbuilder.config import (
    AlternateHrefConfig,
    AssetConfig,
    CollectionConfig,
)

# Modules from this project
from stacbuilder.exceptions import InvalidConfiguration, InvalidOperation
from stacbuilder.metadata import AssetMetadata, BandMetadata

ALTERNATE_ASSETS_SCHEMA = "https://stac-extensions.github.io/alternate-assets/v1.1.0/schema.json"


_logger = logging.getLogger(__name__)


class AlternateHrefGenerator:
    """Generates the alternate links for assets.

    This class is best initialized either by using `AlternateHrefConfig.from_config(config)` or
    creating an instance and calling the `add_local()` or `add_S3()` methods to register
    the callbacks for generating alternate links.
    """

    # Type alias for the specific callable that AlternateHrefGenerator needs.
    AssetMetadataToURL = Callable[[AssetMetadata], str]

    def __init__(self):
        self._callbacks: Dict[str, AlternateHrefGenerator.AssetMetadataToURL] = {}

    def _register_callback(self, key, converter=AssetMetadataToURL):
        self._callbacks[key] = converter

    def _has_alternate_key(self, key: str) -> bool:
        return key in self._callbacks

    def _get_callback(self, key: str) -> Optional[AssetMetadataToURL]:
        """Get the callback for the given key."""
        return self._callbacks.get(key)

    def has_callbacks(self) -> bool:
        """Check if there are any registered callbacks."""
        return bool(self._callbacks)

    def get_alternates(self, asset_metadata: AssetMetadata) -> Dict[str, Dict[str, Dict[str, str]]]:
        """Get the alternate links for the asset metadata.

        Returns a dictionary with the alternate links for the asset metadata.
        The keys are the names of the registered callbacks, and the values are dictionaries with the hrefs.
        If no callbacks are registered, returns an empty dictionary.

        :param asset_metadata: The asset metadata for which to generate the alternate links.
        :return: A dictionary with the alternate links for the asset metadata.
        """
        if not self.has_callbacks():
            return {}
        alternates = {}
        for key in self._callbacks:
            alternates[key] = {"href": self._get_alternate_href_for(key, asset_metadata)}

        return {"alternate": alternates}

    def _get_alternate_href_for(self, key: str, asset_metadata: AssetMetadata) -> Dict[str, str]:
        if not self._has_alternate_key(key):
            raise ValueError(f"No callback registered for key: {key}")
        return self._callbacks[key](asset_metadata)

    def add_local(self):
        """Add a callback for adding an alternate href with a local file path."""
        self._register_callback("local", lambda asset_md: asset_md.asset_path.as_posix())

    def add_S3(self, s3_bucket: str, s3_root_path: Optional[str] = None):
        """Add a callback for adding an alternate href in S3 with an S3 bucket and the asset's file path concatenated to that bucket.

        For example:
            /my/data/folder/some-collection/some-asset.tif
        becomes:
            s3://my-bucket/my/data/folder/some-collection/some-asset.tif

        If you need to translate the file path in a more sophisticated wat you have to write your
        own handler. It is advised to define an item_postprocessor that will
        modify the item before it is saved.
        """
        s3_bucket = self.remove_leading_trailing_slash(s3_bucket)
        s3_root_path = self.remove_leading_trailing_slash(s3_root_path) if s3_root_path else None

        convert = partial(self.to_S3_url, s3_bucket=s3_bucket, s3_root_path=s3_root_path)
        self._register_callback("S3", convert)

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
        """Create an AlternateHrefGenerator from an AlternateHrefConfig."""
        alt_link_gen = AlternateHrefGenerator()
        if not config:
            return alt_link_gen

        if config.add_local:
            alt_link_gen.add_local()

        if config.add_S3:
            if not config.s3_bucket:
                raise InvalidConfiguration(
                    "AlternateHrefConfig specifies S3 links need to be added but there is no value for s3_bucket"
                )
            alt_link_gen.add_S3(s3_bucket=config.s3_bucket, s3_root_path=config.s3_root_path)

        return alt_link_gen


class ItemBuilder:
    """
    Converts AssetMetadata objects to STAC Items.
    This class is used to create STAC Items from AssetMetadata objects.
    """

    def __init__(
        self,
        item_assets_configs: Dict[str, AssetConfig],
        alternate_href_generator: Optional[AlternateHrefGenerator] = None,
    ) -> None:
        # Settings: these are just data, not components we delegate work to.
        self.item_assets_configs: Dict[str, AssetConfig] = item_assets_configs
        self._alternate_href_generator: Optional[AlternateHrefGenerator] = alternate_href_generator

    def create_item(self, assets: List[AssetMetadata]) -> Item:
        """
        Create a STAC Item from a list of AssetMetadata objects. All assets should have the same item_id and consistent metadata.

        :param assets: List of AssetMetadata objects.
        :return: A STAC Item object.
        """

        known_assets = [a for a in assets if a.asset_type in self.item_assets_configs]
        if not known_assets:
            error_msg = (
                "None of the assets is defined in collection configuration. "
                + f"{[a.asset_type for a in assets]} not found in {list(self.item_assets_configs.keys())}"
            )
            _logger.warning(error_msg)
            return None

        first_asset = known_assets[0]  # The first asset is used to create the item.

        # Ensure that all assets have consistent metadata for common properties
        assert all(a.item_id == first_asset.item_id for a in known_assets), "All assets should have the same item_id"
        assert all(a.proj_epsg == first_asset.proj_epsg for a in known_assets), (
            "All assets should have the same proj_epsg"
        )
        assert all(a.transform == first_asset.transform for a in known_assets), (
            "All assets should have the same transform"
        )
        assert all(a.bbox_as_list == first_asset.bbox_as_list for a in known_assets), (
            "All assets should have the same lat-lon bounding box"
        )
        assert all(a.proj_bbox_as_list == first_asset.proj_bbox_as_list for a in known_assets), (
            "All assets should have the same projected bounding box"
        )
        assert all(a.shape == first_asset.shape for a in known_assets), "All assets should have the same shape"

        item = Item(
            id=first_asset.item_id,
            geometry=first_asset.geometry_lat_lon_as_dict,
            bbox=first_asset.bbox_as_list,
            datetime=first_asset.datetime,
            start_datetime=first_asset.start_datetime,
            end_datetime=first_asset.end_datetime,
            properties={},
        )

        item.common_metadata.created = dt.datetime.now(dt.timezone.utc)

        # Add the projection extension
        item_proj = ItemProjectionExtension.ext(item, add_if_missing=True)
        item_proj.apply(
            epsg=first_asset.proj_epsg if first_asset.proj_epsg else None,
            bbox=first_asset.proj_bbox_as_list,
            geometry=first_asset.geometry_proj_as_dict,
            transform=first_asset.transform,
            shape=first_asset.shape,
        )

        for metadata in assets:
            item.add_asset(metadata.asset_type, self._create_asset(metadata, item))

        if self._alternate_href_generator and self._alternate_href_generator.has_callbacks():
            item.stac_extensions.append(ALTERNATE_ASSETS_SCHEMA)

        return item

    def _create_asset(self, metadata: AssetMetadata, item: Item) -> Asset:
        asset_defs = self._get_assets_definitions()
        asset_def: AssetDefinition = asset_defs[metadata.asset_type]
        asset_config = self._get_assets_config_for(metadata.asset_type)
        asset: Asset = asset_def.create_asset(metadata.href)
        asset.set_owner(item)
        asset.media_type = metadata.media_type

        # file extension
        if metadata.file_size:
            file_info = FileExtension.ext(asset, add_if_missing=True)
            file_info.apply(size=metadata.file_size)

        # raster extension
        self._add_raster_bands_to_asset(asset, metadata, asset_config)

        # eo extension
        # the fixed values are set in the AssetDefinition, so we only need to add the extension
        if asset_config.eo_bands:
            asset.ext.add("eo")

        # Add the alternate links for the Alternate-Asset extension
        # see: https://github.com/stac-extensions/alternate-assets
        if metadata.asset_path and self._alternate_href_generator:
            alternate_links = self._alternate_href_generator.get_alternates(metadata)
            if alternate_links:
                asset.extra_fields.update(alternate_links)

        return asset

    def _add_raster_bands_to_asset(self, asset: Asset, metadata: AssetMetadata, asset_config: AssetConfig) -> None:
        """Add raster bands to the asset based on the metadata and asset configuration.

        There are two cases:
        1. If the asset configuration does not specify raster bands, we create them based on the metadata.
        2. If the asset configuration specifies raster bands, the extension is already added in the AssetDefinition,
              and we fill in the missing values from the metadata.
        :param asset: The asset to which the raster bands should be added.
        :param metadata: The metadata containing the band information.
        :param asset_config: The asset configuration containing the band information.
        """
        # Case 1: If the asset configuration does not specify raster bands, we create them based on the metadata.
        if not asset_config.raster_bands:
            # There is no information to fill in default values for raster:bands
            # Just fill in what we do have from asset metadata.
            asset_raster = RasterExtension.ext(asset, add_if_missing=True)
            raster_bands = []
            for band_md in metadata.bands:
                new_band: RasterBand = RasterBand.create(
                    data_type=band_md.data_type,
                    nodata=band_md.nodata,
                    unit=band_md.units,
                )
                raster_bands.append(new_band)
            asset_raster.apply(raster_bands)
        # Case 2: If the asset configuration specifies raster bands, we fill in the missing values from the metadata.
        else:
            raster_bands: List[RasterBand] = asset.ext.raster.bands
            assert len(raster_bands) == len(metadata.bands), (
                f"Number of raster bands in asset metadata ({len(metadata.bands)}) "
                f"does not match the number of raster bands in asset config ({len(raster_bands)})"
            )
            for i, raster_band in enumerate(raster_bands):
                band_md: BandMetadata = metadata.bands[i]

                if not isinstance(raster_band, RasterBand):
                    raise InvalidConfiguration(
                        f"Expected raster band to be of type RasterBand, got {type(raster_band)}"
                    )
                raster_band.apply(
                    data_type=raster_band.data_type or band_md.data_type,
                    nodata=raster_band.nodata or band_md.nodata,
                    unit=raster_band.unit or band_md.units,
                )

    def _get_assets_definitions(self) -> List[AssetDefinition]:
        """Create AssetDefinitions, according to the config in self.item_assets_configs"""
        asset_definitions = {}
        for band_name, asset_config in self.item_assets_configs.items():
            asset_def: AssetDefinition = asset_config.to_asset_definition()
            asset_definitions[band_name] = asset_def

        return asset_definitions

    def _get_assets_config_for(self, asset_type: str) -> AssetConfig:
        """Create AssetDefinitions, according to the config in self.item_assets_configs"""
        if asset_type not in self.item_assets_configs:
            return None
        return self.item_assets_configs[asset_type]


class CollectionBuilder:
    """Class to build a STAC Collection from a list of STAC Items.

    Once initialized, you can use the `build_collection_from_items()` method to create a collection.
    Use the `collection` property to access the created collection."""

    def __init__(
        self,
        collection_config: CollectionConfig,
        output_dir: Optional[Path] = None,
        link_items: Optional[bool] = True,
    ) -> None:
        # Settings: these are just data, not components we delegate work to.
        self._collection_config = collection_config

        self._output_dir = output_dir
        if self._output_dir and not isinstance(self._output_dir, Path):
            self._output_dir = Path(self._output_dir).expanduser().absolute()

        self._link_items = bool(link_items)

        # The result
        self._collection: Collection = None

        self._extent: Optional[Extent] = None

        self._epsg: Optional[int] = None
        self._step: Optional[float] = None

    def reset(self):
        self._collection = None

    @property
    def output_dir(self) -> Path:
        """
        The directory where the collection and the items should be saved
        """
        if not self._output_dir:
            raise InvalidOperation("Output directory is not set.")
        return self._output_dir

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
    def collection_file_path(self) -> Path:
        return self.output_dir / "collection.json"

    @property
    def collection(self) -> Optional[Collection]:
        return self._collection

    def build_collection_from_items(
        self,
        stac_items: Iterable[Item],
        group: Optional[str | int] = None,
        save_collection: bool = True,
    ) -> None:
        """Create and save a STAC Collection from a list of STAC Items.

        The collection will be created with the ID and title from the collection configuration.
        If the collection already exists, it will be reset and a new collection will be created.
        The collection will be saved under the collection property, and optionally to the output directory.

        :param stac_items: An iterable of STAC Items to be added to the collection.
        :param group: Optional group identifier to create a collection with a different ID and title.
        :param save_collection: If True, the collection will be saved to the output directory."""
        self.reset()

        self.create_empty_collection(group=group)

        item_counter = 0
        for item in stac_items:
            if self._collection_config.enable_datacube_extension:
                # Check that all items have the same EPSG code and step size if datacube extension is enabled.
                # If not break the loop as soon as possible.
                self._check_unique_epsg(item.properties.get("proj:epsg"))
                self._check_unique_step(item.properties.get("proj:transform", [None])[0])

            self._process_item(item=item)
            item_counter += 1
            if item_counter % 1000 == 0:
                self._log_progress_message(f"Processed {item_counter} items so far.")

        if self.link_items:
            self._log_progress_message("updating collection extent")
            self._collection.update_extent_from_items()
        else:
            self._log_progress_message("updating collection extent from items")
            if self._extent is None:
                raise InvalidOperation("Extent is not set. Cannot update collection extent without items.")
            self._collection.extent = self._extent

        self.normalize_hrefs()

        if self._collection_config.enable_datacube_extension:
            self._add_datacube_extension()

        if save_collection:
            self.save_collection()

        self._log_progress_message("DONE: build_collection")

        return self.collection

    def _process_item(
        self,
        item: Item,
    ):
        """Fills the collection with stac items."""
        self._log_progress_message("START: add_items_to_collection")

        if self._collection is None:
            raise InvalidOperation("Can not add items to a collection that has not been created yet.")

        if item is None:
            raise ValueError("Argument 'stac_items' can not be None. It must be a list of STAC Items.")
        if not isinstance(item, Item):
            raise TypeError(f"Argument 'stac_items' should be of type Item, got {type(item)}")

        if self.link_items:
            self._collection.add_item(item)
        else:
            # If we do not link items, we save them to the output directory.
            item.collection = self._collection
            item_path = self.get_item_path(item)
            if not item_path.parent.exists():
                item_path.parent.mkdir(parents=True)
            item.save_object(dest_href=item_path.as_posix(), include_self_link=False)
            self._update_extent_from_item(item)

    def _check_unique_epsg(self, epsg: int) -> None:
        """Check that all items have the same EPSG code."""
        if self._epsg is None:
            self._epsg = epsg
        elif self._epsg != epsg:
            raise ValueError(
                f"All items in the collection must have the same EPSG code when datacube extension is enabled. Found both {self._epsg} and {epsg}."
            )

    def _check_unique_step(self, step: float) -> None:
        """Check that all items have the same pixel size (step)."""
        if self._step is None:
            self._step = step
        elif self._step != step:
            raise ValueError(
                f"All items in the collection must have the same pixel size (step) when datacube extension is enabled. Found both {self._step} and {step}."
            )

    def _add_datacube_extension(self) -> None:
        """Add the datacube extension to the collection."""

        if self._collection is None:
            raise InvalidOperation("Collection is not created yet. Cannot add datacube extension.")

        if self._epsg is None or self._step is None:
            raise InvalidOperation("Cannot add datacube extension without EPSG and step size being set.")

        x_extent = [self._extent.spatial.bboxes[0][0], self._extent.spatial.bboxes[0][2]]
        y_extent = [self._extent.spatial.bboxes[0][1], self._extent.spatial.bboxes[0][3]]
        temporal_extent = [self._extent.temporal.intervals[0][0], self._extent.temporal.intervals[0][1]]

        x_dimension = SpatialDimension(
            properties={"extent": x_extent, "reference_system": CRS.from_epsg(self._epsg).to_json(), "step": self._step}
        )
        y_dimension = SpatialDimension(
            properties={"extent": y_extent, "reference_system": CRS.from_epsg(self._epsg).to_json(), "step": self._step}
        )
        t_dimension = TemporalDimension(properties={"extent": temporal_extent})
        band_dimension = AdditionalDimension(
            properties={"type": "bands", "values": list(self._collection_config.item_assets.keys())}
        )

        datacube_ext = DatacubeExtension.ext(self._collection, add_if_missing=True)
        datacube_ext.dimensions = {"x": x_dimension, "y": y_dimension, "t": t_dimension, "bands": band_dimension}

    def _update_extent_from_item(self, item: Item):
        """Update the extent of the collection based on the item."""
        if self._extent is None:
            self._extent = Extent.from_items(items=[item])
        else:
            bounds = item.bbox
            starts = item.common_metadata.start_datetime or item.datetime
            ends = item.common_metadata.end_datetime or item.datetime

            self._extent.spatial = SpatialExtent(
                [
                    [
                        min(self._extent.spatial.bboxes[0][0], bounds[0]),
                        min(self._extent.spatial.bboxes[0][1], bounds[1]),
                        max(self._extent.spatial.bboxes[0][2], bounds[2]),
                        max(self._extent.spatial.bboxes[0][3], bounds[3]),
                    ]
                ]
            )
            self._extent.temporal = TemporalExtent(
                [
                    [
                        min(self._extent.temporal.intervals[0][0], starts),
                        max(self._extent.temporal.intervals[0][1], ends),
                    ]
                ]
            )

    def stac_item_dir(self) -> Path:
        return self.output_dir / self.collection.id

    def get_item_path(self, item: Item) -> Path:
        year = f"{item.datetime.year:04}"
        month = f"{item.datetime.month:02}"
        day = f"{item.datetime.day:02}"
        return self.stac_item_dir() / year / month / day / f"{item.id}.json"

    def normalize_hrefs(self):
        layout_template = self._collection_config.layout_strategy_item_template
        strategy = TemplateLayoutStrategy(item_template=layout_template)

        out_dir_str = self.output_dir.as_posix()
        if out_dir_str.endswith("/"):
            out_dir_str = out_dir_str[-1]
        self._collection.normalize_hrefs(root_href=out_dir_str, strategy=strategy, skip_unresolved=False)

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

        self._collection.links.sort(key=lambda x: repr(x))

        if not self.output_dir.exists():
            self.output_dir.mkdir(parents=True)

        self._collection.save(catalog_type=CatalogType.SELF_CONTAINED)
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
        )
        # TODO: Add support for summaries: https://github.com/VitoTAP/stac-catalog-builder/issues/18
        #   Summaries should, among other things, contain the platforms, instruments and mission, when available.
        #   In STAC these are singular but in fact there can be multiple.
        #   If there are multiple values we encode it as a string containing comma-separated values.

        item_assets_ext = ItemAssetsExtension.ext(collection, add_if_missing=True)
        item_assets_ext.item_assets = self._get_item_assets_definitions()

        RasterExtension.add_to(collection)
        EOExtension.add_to(collection)

        self._collection = collection
        self._log_progress_message("DONE: create_empty_collection")

    def get_default_extent(self) -> Extent:
        end_dt = dt.datetime.now(dt.timezone.utc)

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


class AssetMetadataPipeline:
    """
    Creates a STAC Collection and Items from AssetMetadata. This class is the main entry point for the STAC catalog builder.

    The AssetMetadata is collected by the IMetadataCollector implementation, which is passed to the constructor.
    """

    def __init__(
        self,
        metadata_collector: IMetadataCollector,
        collection_config: CollectionConfig,
        output_dir: Optional[Path] = None,
        link_items: Optional[bool] = True,
        item_postprocessor: Optional[Callable] = None,
    ) -> None:
        if output_dir and not isinstance(output_dir, Path):
            raise TypeError(f"Argument output_dir (if not None) should be of type Path, {type(output_dir)=}")

        if collection_config is None:
            raise ValueError('Argument "collection_config" can not be None, must be a CollectionConfig instance.')

        if metadata_collector is None or not isinstance(metadata_collector, IMetadataCollector):
            raise ValueError(
                'Argument "metadata_collector" can not be None, must be a IMetadataCollector implementation.'
            )
        # Settings: these are just data, not components we delegate work to.
        self._collection_dir: Path = output_dir
        self._link_items = bool(link_items)
        self.collection_config: CollectionConfig = collection_config

        # Components / dependencies that must be provided
        self.metadata_collector: IMetadataCollector = metadata_collector

        # Components / dependencies that we set up internally
        self.item_builder: ItemBuilder = ItemBuilder(
            item_assets_configs=self.item_assets_configs,
            alternate_href_generator=AlternateHrefGenerator.from_config(self.collection_config.alternate_links),
        )
        self._func_find_item_group: Optional[Callable[[Item], str]] = None

        self.collection_builder: CollectionBuilder = CollectionBuilder(
            collection_config=self.collection_config,
            output_dir=self._collection_dir,
            link_items=self._link_items,
        )

        self.item_postprocessor: Optional[Callable] = item_postprocessor

        # results
        self.collection: Optional[Collection] = None
        self.collection_groups: Dict[Hashable, Collection] = {}

    @staticmethod
    @deprecated.deprecated(
        "Use AssetMetadataPipeline() instead. This method will be removed in a future version.",
    )
    def from_config(
        metadata_collector: IMetadataCollector,
        collection_config: CollectionConfig,
        output_dir: Optional[Path] = None,
        link_items: Optional[bool] = True,
        item_postprocessor: Optional[Callable] = None,
    ) -> "AssetMetadataPipeline":
        """Creates a AssetMetadataPipeline from configurations."""
        return AssetMetadataPipeline(
            metadata_collector=metadata_collector,
            collection_config=collection_config,
            output_dir=output_dir,
            link_items=link_items,
            item_postprocessor=item_postprocessor,
        )

    @property
    def collection_file(self) -> Path | None:
        if not self.collection:
            return None
        return Path(self.collection.self_href)

    @property
    def item_assets_configs(self) -> Dict[str, AssetConfig]:
        return self.collection_config.item_assets or {}

    def reset(self) -> None:
        """Reset the internal state of the pipeline."""
        self.collection = None
        self.collection_groups = {}

    def get_metadata(self) -> Iterable[AssetMetadata]:
        """Tells the metadata collector to collect the metadata and return it."""
        self.metadata_collector.collect()
        _logger.info("Metadata collection done.")
        return self.metadata_collector.metadata_list

    def collect_stac_items(self):
        """Generate the intermediate STAC Item objects."""
        self._log_progress_message("START: collect_stac_items")

        groups = self._group_metadata_by_item_id(self.get_metadata())
        num_groups = len(groups)

        progress_chunk_size = 10_000
        for i, assets in enumerate(groups.values()):
            if i % progress_chunk_size == 0:
                fraction_done = i / num_groups
                self._log_progress_message(
                    f"Converted {i} of {num_groups} AssetMetadata to STAC Items ({fraction_done:.1%})"
                )
            sub_groups = self._split_group_by_latlon(
                assets
            )  # Ensure that all the assets have the same lat-lon bounding box
            for sub_group_assets in sub_groups.values():
                stac_item = self.item_builder.create_item(sub_group_assets)
                if stac_item:
                    if self.item_postprocessor is not None:
                        stac_item = self.item_postprocessor(stac_item)
                    yield stac_item

        # Clean up the memory
        del groups
        self.metadata_collector.reset()
        gc.collect()
        self._log_progress_message("DONE: collect_stac_items")

    def _group_metadata_by_item_id(self, iter_metadata: Iterable[AssetMetadata]) -> Dict[str, List[Item]]:
        self._log_progress_message("START: group_metadata_by_item_id")
        groups: Dict[str, AssetMetadata] = {}

        for metadata in iter_metadata:
            item_id = metadata.item_id

            if item_id not in groups:
                groups[item_id] = []

            groups[item_id].append(metadata)

        self._log_progress_message("DONE: group_metadata_by_item_id")
        return groups

    def _split_group_by_latlon(self, metadata_list: List[AssetMetadata]) -> Dict[Tuple[int, int], List[AssetMetadata]]:
        """Split the metadata into groups, based on the lat-lon bounding box."""
        groups: Dict[Tuple[int, int], List[AssetMetadata]] = {}

        for metadata in metadata_list:
            latlon = metadata.bbox_as_list
            if latlon is None:
                continue
            latlon = tuple(latlon)
            if latlon not in groups:
                groups[latlon] = []
            groups[latlon].append(metadata)

        return groups

    def build_collection(
        self,
    ):
        """Build the entire STAC collection."""
        self._log_progress_message("START: build_collection")

        assert self._collection_dir, "Collection directory must be set before building the collection."

        self.reset()

        # This calls the item builder to create STAC Items from AssetMetadata.
        item_generator: Generator[Item] = self.collect_stac_items()

        # This passes the STAC Items to the collection builder to create a STAC Collection.
        self.collection = self.collection_builder.build_collection_from_items(item_generator, save_collection=True)

    ####################################################
    # Code below is specific to the grouped collections.
    ####################################################

    def _setup_internals_for_group(
        self,
        group: str | int,
    ) -> None:
        """Setup the internal components based on the components that we receive via dependency injection."""
        # The default way we group items into multiple collections is by year
        # Currently we don't use any other ways to create a group of collections.
        if group is None:
            raise ValueError("Argument 'group' can not be None. It must be a string or an integer.")

        if not self.uses_collection_groups:
            raise InvalidOperation("You can only use collection groups when the pipeline is configured for grouping.")

        self._collection_dir = self.get_collection_file_for_group(group)

        self.collection_builder = CollectionBuilder(
            collection_config=self.collection_config,
            output_dir=self._collection_dir,
            link_items=self._link_items,
        )

    @property
    def uses_collection_groups(self):
        return self._func_find_item_group is not None

    def get_collection_file_for_group(self, group: str | int):
        return self._output_base_dir / str(group)

    def group_stac_items_by(self) -> Dict[int, List[Item]]:
        """Group the STAC items by calling the function that is set in _func_find_item_group."""
        groups: Dict[int, AssetMetadata] = {}
        iter_items = self.collect_stac_items()

        for item in iter_items:
            group = self._func_find_item_group(item)

            if group not in groups:
                groups[group] = []

            groups[group].append(item)

        return groups

    def build_grouped_collections(self):
        self._log_progress_message("START: build_grouped_collections")

        assert self._collection_dir, "Collection directory must be set before building grouped collections."

        self._func_find_item_group = lambda item: item.datetime.year  # Default grouping by year
        self._output_base_dir = self._collection_dir

        self.reset()
        self._root_collection_builder = CollectionBuilder(
            collection_config=self.collection_config,
            output_dir=self._output_base_dir,
        )
        self._root_collection_builder.create_empty_collection()

        for group, metadata_list in sorted(self.group_stac_items_by().items()):
            self._setup_internals_for_group(group=group)

            self.collection_builder.build_collection_from_items(
                stac_items=metadata_list,
                group=group,
                save_collection=False,
            )

            self._root_collection_builder.collection.add_child(self.collection_builder.collection)
            self.collection_groups[group] = self.collection_builder.collection

        self._root_collection_builder.normalize_hrefs()
        self._root_collection_builder.collection.update_extent_from_items()
        self._root_collection_builder.save_collection()

        self._log_progress_message("DONE: build_grouped_collections")

    def _log_progress_message(self, message: str) -> None:
        calling_method_name = inspect.stack()[1][3]
        _logger.info(f"PROGRESS: {self.__class__.__name__}.{calling_method_name}: {message}")
