"""
Support for extracting data from HRVPP.

This is done via the terracatalogueclient.
"""

import datetime as dt
import gc
import inspect
import itertools
import logging
from pathlib import Path
from pprint import pprint
from typing import Any, Dict, List, Optional, Tuple

import geopandas as gpd
import pandas as pd
import psutil
from pystac.media_type import MediaType
from pystac.provider import ProviderRole

try:
    import terracatalogueclient as tcc
    from terracatalogueclient.config import CatalogueConfig, CatalogueEnvironment
except ImportError:
    raise ImportError(
        "Terracatalogueclient not found. Please install it with 'pip install terracatalogueclient==0.1.14 --extra-index-url https://artifactory.vgt.vito.be/artifactory/api/pypi/python-packages/simple'."
    )


from stacbuilder.async_utils import AsyncTaskPoolMixin
from stacbuilder.boundingbox import BoundingBox
from stacbuilder.collector import IMetadataCollector
from stacbuilder.config import (
    AssetConfig,
    CollectionConfig,
    EOBandConfig,
    ProviderModel,
    RasterBandConfig,
)
from stacbuilder.metadata import AssetMetadata
from stacbuilder.projections import reproject_bounding_box

_logger = logging.getLogger(__name__)


# EPSG code for "lat-long", or WGS84 to be more precise;
# Using a constant in order to avoid magic numbers.
EPSG_4326_LATLON = 4326


def get_coll_temporal_extent(collection: tcc.Collection) -> Tuple[dt.datetime | None, dt.datetime | None]:
    acquisitionInformation = collection.properties["acquisitionInformation"]

    dt_start = None
    dt_end = None
    for info in acquisitionInformation:
        new_dt_start = info.get("acquisitionParameters", {}).get("beginningDateTime")
        new_dt_end = info.get("acquisitionParameters", {}).get("endingDateTime")

        new_dt_start = dt.datetime.fromisoformat(new_dt_start)
        new_dt_end = dt.datetime.fromisoformat(new_dt_end)

        if not dt_start:
            dt_start = new_dt_start
        elif new_dt_start < dt_start:
            dt_start = new_dt_start

        if not dt_end:
            dt_end = new_dt_end
        elif dt_end < new_dt_end:
            dt_end = new_dt_end

    return dt_start, dt_end


class CollectionConfigBuilder:
    """Creates CollectionConfig objects from collection info we receive from terracatalogueclient."""

    def __init__(self, tcc_collection: tcc.Collection):
        self.tcc_collection = tcc_collection

    def get_collection_config(self) -> CollectionConfig:
        """Create a CollectionConfig for the Current tcc collection."""
        coll = self.tcc_collection
        collection_id = coll.id
        title = coll.properties.get("title")
        description = coll.properties.get("abstract")

        keywords = coll.properties.get("keyword")
        providers = [
            ProviderModel(
                name="VITO",
                roles=[ProviderRole.LICENSOR, ProviderRole.PRODUCER, ProviderRole.PROCESSOR],
                url="https://www.vito.be/",
            )
        ]

        platforms = self.get_platforms()
        instrument = [self.get_instruments()]
        # TODO: Do we have a field for mission in OpenSearch / terracatalogueclient?
        # mission = Optional[List[str]] = []

        layout_strategy_item_template = "${collection}/${year}/${month}/${day}"
        item_assets = self.get_asset_config()

        return CollectionConfig(
            collection_id=collection_id,
            title=title,
            description=description,
            keywords=keywords or None,
            providers=providers or None,
            platform=platforms or None,
            instrument=instrument or None,
            layout_strategy_item_template=layout_strategy_item_template,
            item_assets=item_assets,
        )

    def get_platforms(
        self,
    ) -> Optional[List[str]]:
        """Get the platform or platforms

        Mapping:
        - terracatalogueclient: properties.acquisitionInformation.platform.platformShortName
        - STAC Collection: platform (singular)
            https://github.com/radiantearth/stac-spec/blob/master/item-spec/common-metadata.md#instrument
            pystac: Item: item.common_metadata.platform

        In case we have multiple platforms, we will concatenate them as a comma-separated list in the JSON representation.
        """
        platforms = []
        acq_info = self.get_acquisition_info()
        for info in acq_info:
            platform = info.get("platform")
            if platform:
                platforms.append(platform.get("platformShortName"))

        return list(set(platforms))

    def get_instruments(self) -> Optional[List[str]]:
        """Get the platform or platforms

        Mapping:
        - terracatalogueclient: properties.acquisitionInformation.instrument.instrumentShortName
        - STAC Collection: instruments
            https://github.com/radiantearth/stac-spec/blob/master/item-spec/common-metadata.md#instrument
            pystac: Item: item.common_metadata.instruments
        """
        instruments = []
        acq_info = self.get_acquisition_info()
        for info in acq_info:
            instrument = info.get("instrument", {})
            if instrument:
                instruments.append(instrument.get("instrumentShortName"))

        return list(set(instruments))

    def get_acquisition_info(self) -> Dict[str, Any]:
        """Helper method to get acquisitionInformation property"""
        return self.tcc_collection.properties.get("acquisitionInformation", [])

    def get_product_info(self) -> Dict[str, Any]:
        """Helper method to get productInformation property"""
        return self.tcc_collection.properties.get("productInformation", {})

    def get_file_format(self) -> Optional[str]:
        """Get the file format, AKA the MediaType in pystac terms"""
        prod_info = self.get_product_info()
        return prod_info.get("format")

    def get_asset_config(self) -> List[AssetConfig]:
        """Create an AssetConfig object from band band information in the current tcc.Collection"""
        media_type = self.get_media_type()

        # TODO: eo:bands will also need the wavelength, from properties.acquisitionInformation.instrument.wavelengths.discreteWavelengths
        asset_configs = {}
        band_info = self.tcc_collection.properties.get("bands")
        for band in band_info:
            title = band.get("title")
            offset = band.get("offset")
            scale_factor = band.get("scaleFactor")
            bit_per_value = band.get("bitPerValue")
            spatial_resolution = band.get("resolution")
            data_type = self.guess_datatype(bit_per_value)

            raster_cfg = RasterBandConfig(
                name=title,
                offset=offset,
                scale=scale_factor,
                spatial_resolution=spatial_resolution,
                bits_per_sample=bit_per_value,
                data_type=data_type,
            )
            eobands_cfg = EOBandConfig(
                name=title,
                description=title,
            )
            asset_cfg = AssetConfig(
                title=title,
                description=title,
                media_type=media_type,
                raster_bands=[raster_cfg],
                eo_bands=[eobands_cfg],
            )
            asset_configs[title] = asset_cfg

        return asset_configs

    def guess_datatype(self, bit_per_value: int) -> str:
        """We have to make an educated guess for datatype because is not clearly specified in the tcc.Collection.
        To be improved
        """
        # TODO: need to add setting so we know if we should assume it is an int type or a a float type
        #   While float types with less than 32 bits do exist, they are not common.
        #   Not sure if EO ever uses those, but they do exist in other industries
        #   (for example it is commonly used in the EXR image format, but I haven't heard of any other examples)
        #   So for 16 bits and less we could assume it must be an int.
        #    For 32 and 64 bit it could be either float or int.
        return f"uint{bit_per_value}"

    def get_media_type(self) -> MediaType:
        """Get the corresponding pystac MediaType value for the file type."""
        format = self.get_file_format()
        media_type = None
        if format.lower() in ["geotif", "geotiff", "tiff"]:
            media_type = MediaType.GEOTIFF
        elif format.lower() == "COG":
            media_type = MediaType.COG
        return media_type

    def get_product_types(self):
        """List the product types that are available in this collection."""
        return self.get_product_info().get("productType")


class HRLVPPMetadataCollector(AsyncTaskPoolMixin, IMetadataCollector):
    """Collects AssetMetadata for further processing for the HRL VPP collections from OpenSearch."""

    def __init__(
        self,
        temp_dir: Path | None = None,
        query_by_frequency: str | None = "QS",
        save_intermediates: bool = False,
        slice_length: int = 100,
    ):
        super().__init__()
        self._init_async_task_pool(max_outstanding_tasks=10_000)

        # components: objects that we delegate work to.
        self._cfg_builder: CollectionConfigBuilder = None

        # state / collected information
        self._products_df: Optional[gpd.GeoDataFrame] = None

        self._collection_id: Optional[str] = None
        self._max_products = -1
        self._query_by_frequency: str = query_by_frequency or "QS"
        self._save_intermediates: bool = save_intermediates
        self._slice_length: int = slice_length

        self.temp_dir: Path = Path(temp_dir) if temp_dir else None

    @property
    def collection_id(self) -> Optional[str]:
        """The ID of the collection we want to process."""
        return self._collection_id

    @collection_id.setter
    def collection_id(self, collection_id) -> None:
        if not isinstance(collection_id, (str, None)):
            raise TypeError(
                "Value for collection_id must be either None or type str."
                + f"{type(collection_id)=}, {collection_id=!r}"
            )
        self._collection_id = collection_id

    @property
    def max_products(self) -> int:
        """Set max_products to a value > 0 to process only that many products.

        There are a large amount of products per collection (order of 10k)
        This is a meant to help develop an troubleshoot with a lot less products.
        """
        return self._max_products

    @max_products.setter
    def max_products(self, value: int) -> None:
        if not isinstance(value, int):
            raise TypeError(f"Value for max_products must be an int. {type(value)=}, {value=}")
        self._max_products = int(value) if value else -1

    def _log_progress_message(self, message: str, level=logging.INFO) -> None:
        calling_method_name = inspect.stack()[1][3]
        match level:
            case logging.DEBUG:
                _logger.debug(f"PROGRESS: {self.__class__.__name__}.{calling_method_name}: {message}")
            case logging.INFO:
                _logger.info(f"PROGRESS: {self.__class__.__name__}.{calling_method_name}: {message}")
            case logging.WARNING:
                _logger.warning(f"PROGRESS: {self.__class__name__}.{calling_method_name}: {message}")
            case logging.ERROR:
                _logger.error(f"PROGRESS: {self.__class__name__}.{calling_method_name}: {message}")
            case _:
                _logger.info(f"PROGRESS: {self.__class__.__name__}.{calling_method_name}: {message}")

    def collect(self) -> None:
        """Collect and store the AssetMetadata objects."""
        self._log_progress_message("START: collect")

        if self.has_collected():
            _logger.info("Already collected data. Returning")
            return

        if not self._products_df:
            self._log_progress_message(
                "Downloading products to dataframe. Max products to retrieve: {self.max_products}"
            )
            self.get_products_as_dataframe()
            # self._save_dataframes()

        _logger.info("PROGRESS: converting GeoDataFrame to list of AssetMetadata objects")
        self._metadata_list = self._convert_to_asset_metadata(self._products_df)

        # Free up memory
        self._products_df = None
        gc.collect()

        self._log_progress_message("DONE: collect")

    def _save_dataframes(self) -> None:
        if self.temp_dir:
            if not self.temp_dir.exists():
                self.temp_dir.mkdir(parents=True)

            if self._products_df is not None:
                self._save_geodataframe(self._products_df, f"asset_metadata-{self.collection_id}")

    def _save_geodataframe(self, gdf: gpd.GeoDataFrame, table_name: str) -> Path:
        if not self.temp_dir.exists():
            self.temp_dir.mkdir(parents=True)

        geodataframe_path = self.temp_dir / f"{table_name}.parquet"
        self._log_progress_message(f"Saving {table_name} as GeoDataFrame (geoparquet), path={geodataframe_path}")
        gdf.to_parquet(path=geodataframe_path, index=True)
        self._log_progress_message(f"DONE saved download products to {geodataframe_path}")
        return geodataframe_path

    def _get_intermediate_relative_path(self, index: str) -> Path:
        return Path(f"intermediates/{index}-asset_metadata-{self.collection_id}")

    def _store_asset_metadata_and_clear_memory(self, index: int) -> Optional[Path]:
        """Store the AssetMetadata objects to disk and clear the memory."""
        if not self.temp_dir:
            return None

        self._log_progress_message("Storing intermediate results to disk", level=logging.INFO)
        intermediates_dir = self.temp_dir / "intermediates"
        intermediates_dir.mkdir(parents=True, exist_ok=True)

        gdf_path = self._save_geodataframe(self._products_df, self._get_intermediate_relative_path(index))
        self._products_df = None
        gc.collect()
        return gdf_path

    def _is_intermediate_stored(self, index: int) -> bool:
        if self.temp_dir:
            gdf_path = self.temp_dir / self._get_intermediate_relative_path(index).with_suffix(".parquet")
            return gdf_path.exists()
        return False

    def _get_num_products_from_intermediate(self, index: int) -> int:
        if self._is_intermediate_stored(index):
            gdf_path = self.temp_dir / self._get_intermediate_relative_path(index).with_suffix(".parquet")
            return len(gpd.read_parquet(gdf_path))

    def _restore_asset_metadata_from_disk(self, paths: List[Path]) -> None:
        """Restore the AssetMetadata objects from disk."""
        if self.temp_dir:
            self._log_progress_message(f"Restoring AssetMetadata products from disk. {paths}", level=logging.INFO)
            for path in paths:
                if not path.exists():
                    self._log_progress_message(f"Path {path} does not exist. Skipping.", level=logging.WARNING)
                    continue
                if self._products_df is None:
                    self._products_df = gpd.read_parquet(path)
                else:
                    self._products_df = pd.concat([self._products_df, gpd.read_parquet(path)])
                    # self._df_asset_metadata = self._df_asset_metadata.drop_duplicates(subset=["asset_id"])
                self._log_progress_message(
                    f"Restored {path} from disk. Memory usage: {psutil.Process().memory_info().rss // 1024**2:_}MB",
                    level=logging.DEBUG,
                )
            self._log_progress_message(
                f"Restored {len(self._products_df):_} AssetMetadata products from disk.", level=logging.INFO
            )

    def get_tcc_catalogue(self) -> tcc.Catalogue:
        """Get the terracatalogueclient's Catalogue to query data from."""
        config = CatalogueConfig.from_environment(CatalogueEnvironment.HRVPP)
        return tcc.Catalogue(config)

    def get_tcc_collections(self) -> List[tcc.Collection]:
        """Get a list of available collections from the terracatalogueclient."""
        catalogue = self.get_tcc_catalogue()
        return list(catalogue.get_collections())

    def get_tcc_collection(self) -> tcc.Collection:
        """Get the collection from the terracatalogueclient (tcc)

        Note This is *not* a STAC/pystac collection, but a class from the terracatalogueclient.
        """
        for coll in self.get_tcc_collections():
            if coll.id == self.collection_id:
                return coll
        return None

    def get_collection_config(self) -> CollectionConfig:
        """Build a CollectionConfig based on the collection info we receive from terracatalogueclient.

        Most of the information can be retrieved from the terracatalogueclient,
        and for these collections it may be a lot more work to set up a CollectionConfig by hand.
        Fortunately we don't need to.
        """
        tcc_collection = self.get_tcc_collection()
        if not tcc_collection:
            return None

        self._cfg_builder = CollectionConfigBuilder(self.get_tcc_collection())
        return self._cfg_builder.get_collection_config()

    def _get_product_query_slots(self, frequency: str = "D") -> tuple[tuple[dt.datetime, dt.datetime], str]:
        """Get the parameters to retrieve product metadata in a piecemeal fashion,
        by iterating over the time periods and product types.

        :param frequency:
            How long each time slot should be, expressed as a frequency alias in Pandas,
            See: https://pandas.pydata.org/docs/user_guide/timeseries.html#timeseries-offset-aliases
            defaults to "D"
        :return: a list of tuples, containing:
            1) a pair (2-tuple) with the start and end datetime
            2) the product type (str)
        """
        collection = self.get_tcc_collection()
        # We retrieve the products in chunks per day, and per product type (see below)
        # So divide the temporal extent into slots per day:
        dt_start, dt_end = get_coll_temporal_extent(collection)
        dt_ranges = pd.date_range(dt_start, dt_end, freq=frequency)

        prod_types = self._cfg_builder.get_product_types()
        time_slots = zip(dt_ranges[:-1], dt_ranges[1:])

        return list(itertools.product(time_slots, prod_types))

    def list_num_prods_per_query_slot(self, collection_id: str, frequency: str = "MS") -> list[dict]:
        results = []
        catalogue = self.get_tcc_catalogue()
        self._cfg_builder = CollectionConfigBuilder(self.get_tcc_collection())

        for (slot_start, slot_end), prod_type in self._get_product_query_slots(frequency=frequency):
            number = catalogue.get_product_count(collection_id, start=slot_start, end=slot_end, productType=prod_type)
            record = dict(
                collection_id=collection_id, start=slot_start, end=slot_end, productType=prod_type, num_product=number
            )
            results.append(record)
            pprint(record)

        return results

    def get_products_as_dataframe(self) -> gpd.GeoDataFrame:
        """Collect the products / assets info from the terracatalogueclient into a GeoDataframe.

        This allows us to retrieve all products first and then process them. This makes it easier
        to group the products that belong to one STAC item, because we don't have much control
        over what order we receive them.
        """
        self._log_progress_message("START: get_products_as_dataframe")

        self._products_df = None
        catalogue = self.get_tcc_catalogue()
        collection = self.get_tcc_collection()
        product_ids = set()
        num_prods = catalogue.get_product_count(collection.id)
        max_prods_to_process = self._max_products if self._max_products > 0 else num_prods

        _logger.info(f"product count for coll_id {collection.id}: {num_prods}")

        query_slots = self._get_product_query_slots(frequency=self._query_by_frequency)
        limit_reached = False
        intermediate_paths = []
        total_products_processed = 0

        for slice_idx in range(0, len(query_slots), self._slice_length):
            if limit_reached:
                break

            # Check if we can reuse intermediate results
            if self._save_intermediates and self._is_intermediate_stored(slice_idx):
                num_products_in_slice = self._get_num_products_from_intermediate(slice_idx)
                intermediate_paths.append(
                    self.temp_dir / self._get_intermediate_relative_path(slice_idx).with_suffix(".parquet")
                )
                total_products_processed += num_products_in_slice
                self._log_progress_message(f"Slot {slice_idx} already stored ({num_products_in_slice:_} products).")
                continue

            # Process a slice of query slots
            query_slots_slice = query_slots[slice_idx : slice_idx + self._slice_length]
            futures = [
                self._submit_async_task(self._fetch_timeslot, slot_start, slot_end, prod_type)
                for (slot_start, slot_end), prod_type in query_slots_slice
            ]

            # Wait for all futures in this chunk to complete
            for future in futures:
                products = future.result()
                new_products = [p for p in products if p.id not in product_ids]

                if new_products:
                    product_ids.update(p.id for p in new_products)
                    self._add_items_to_gdf(new_products)

            # Wait for all tasks in this slice to complete before continuing
            self._wait_for_tasks(shutdown=False)

            total_products_processed = len(self._products_df) if self._products_df is not None else 0
            percent_processed = total_products_processed / max_prods_to_process
            self._log_progress_message(
                f"Progress: {total_products_processed} of {max_prods_to_process} "
                f"({percent_processed:.1%}) Memory: {psutil.Process().memory_info().rss // 1024**2:_}MB"
            )

            if total_products_processed >= max_prods_to_process:
                limit_reached = True
                break

            # Optionally save intermediate results
            if self._save_intermediates:
                path = self._store_asset_metadata_and_clear_memory(slice_idx)
                if path:
                    intermediate_paths.append(path)
                    total_products_processed = 0  # Reset since we cleared memory

            gc.collect()

        # Shutdown the executor now that all processing is complete
        self.shutdown_executor()

        # Restore from intermediates if we saved them
        if self._save_intermediates and intermediate_paths:
            self._restore_asset_metadata_from_disk(intermediate_paths)

        # Sanity checks
        self._log_progress_message("Running sanity checks...")
        final_product_ids = set(self._products_df.index)

        # Check for duplicates
        if len(final_product_ids) != len(self._products_df):
            self._log_progress_message(
                f"Duplicate products found: {len(self._products_df)} rows but only {len(final_product_ids)} unique IDs",
                level=logging.ERROR,
            )

        # Check we got all products (only when not limiting)
        if not self.max_products and len(final_product_ids) != num_prods:
            self._log_progress_message(
                f"Product count mismatch: expected {num_prods}, got {len(final_product_ids)}",
                level=logging.ERROR,
            )

        self._log_progress_message("DONE: get_products_as_dataframe", level=logging.DEBUG)
        return self._products_df

    def _fetch_timeslot(self, slot_start: dt.datetime, slot_end: dt.datetime, prod_type: str) -> List[tcc.Product]:
        catalogue = self.get_tcc_catalogue()
        collection = self.get_tcc_collection()

        num_prods_in_slot = catalogue.get_product_count(
            collection.id, start=slot_start, end=slot_end, productType=prod_type
        )
        self._log_progress_message(
            f"Retrieving products for time slot from {slot_start} to {slot_end}, {prod_type=}, number of products in slot: {num_prods_in_slot}",
            level=logging.DEBUG,
        )
        products = list(
            catalogue.get_products(
                collection.id,
                start=slot_start,
                end=slot_end,
                productType=prod_type,
                accessedFrom="S3",
            )
        )
        self._log_progress_message(
            f"# Retrieved {len(products)} products for time slot {slot_start} to {slot_end}, {prod_type=}",
            level=logging.DEBUG,
        )
        return products

    def _add_items_to_gdf(self, new_products):
        self._log_progress_message("START: adding new assets (deferred AssetMetadata objects) ...", level=logging.DEBUG)
        # We now only build lightweight dict rows + geometry; AssetMetadata instantiation is deferred
        records = []
        geometries = []
        for product in new_products:
            row_dict, geom = self._build_row_dict(product)
            records.append(row_dict)
            geometries.append(geom)

        gdf_new = gpd.GeoDataFrame(data=records, crs=EPSG_4326_LATLON, geometry=geometries)
        gdf_new.index = gdf_new["asset_id"]
        gdf_new.sort_index()

        if self._products_df is None:
            self._products_df = gdf_new
        else:
            self._products_df = pd.concat([self._products_df, gdf_new])
            # Drop duplicates early to keep memory lower
            self._products_df = self._products_df[~self._products_df.index.duplicated(keep="first")]

        self._log_progress_message("DONE: adding new assets (deferred AssetMetadata objects)", level=logging.DEBUG)

    def _build_row_dict(self, product: tcc.Product) -> tuple[dict, Any]:
        """Return minimal dict + geometry for a product. Mirrors create_asset_metadata but avoids object creation.

        This reduces per-product overhead (no dataclass / validation) and defers any heavy logic to final conversion.
        """
        props = product.properties
        data_links = props.get("links", {}).get("data", [])
        first_link = data_links[0]
        first_prod_data = product.data[0] if product.data else None

        href = first_prod_data.href if first_prod_data else first_link.get("href")
        if data_links and first_prod_data:
            # quick consistency assertion kept light (can downgrade to debug later)
            try:
                assert first_link.get("href") == href
            except AssertionError:
                _logger.debug("Mismatch between data link href and product data href for %s", product.id)

        product_type = props.get("productInformation", {}).get("productType")
        num_chars_to_remove = 1 + len(product_type) if product_type else 0
        item_id = product.id[:-num_chars_to_remove] if num_chars_to_remove else product.id

        acquisitionInformation = props.get("acquisitionInformation") or []
        tile_id = None
        if acquisitionInformation:
            tile_id = acquisitionInformation[0].get("acquisitionParameters", {}).get("tileId")

        asset_type = first_prod_data.title if first_prod_data else None
        media_type = AssetMetadata.mime_to_media_type(first_prod_data.type) if first_prod_data else None
        file_size = first_prod_data.length if first_prod_data else None

        # EPSG detection (same logic, condensed)
        epsg_code = None
        if data_links:
            epsg_url = first_link.get("conformsTo")
            if epsg_url and "EPSG" in epsg_url:
                parts = epsg_url.split("/")
                try:
                    epsg_code = int(parts[-1])
                except Exception:
                    _logger.debug("Could not parse EPSG from %s", epsg_url, exc_info=True)
        if epsg_code is None and tile_id:
            import re

            results = re.findall(r"\d+", tile_id)
            if results:
                epsg_code = int("326" + results[0])
        if epsg_code is None:
            _logger.debug("Could not determine EPSG code for product %s; defaulting to 4326", product.id)
        proj_epsg = epsg_code or EPSG_4326_LATLON

        bbox_lat_lon_list = product.bbox
        # Defer creation of BoundingBox objects; store raw values to minimize overhead
        # We'll reconstruct BoundingBox in final conversion step.

        record = {
            "href": href,
            "original_href": href,
            "asset_id": product.id,
            "item_id": item_id,
            "tile_id": tile_id,
            "asset_type": asset_type,
            "media_type": media_type,
            "file_size": file_size,
            "proj_epsg": proj_epsg,
            "bbox_lat_lon_minx": bbox_lat_lon_list[0],
            "bbox_lat_lon_miny": bbox_lat_lon_list[1],
            "bbox_lat_lon_maxx": bbox_lat_lon_list[2],
            "bbox_lat_lon_maxy": bbox_lat_lon_list[3],
            "datetime": product.beginningDateTime,
            "start_datetime": product.beginningDateTime,
            "end_datetime": product.endingDateTime,
        }
        # geometry kept separate for GeoDataFrame geometry column
        geometry = product.geometry
        return record, geometry

    def _convert_to_asset_metadata(self, df: pd.DataFrame) -> List[AssetMetadata]:
        """Convert the pandas dataframe to a list of AssetMetadata objects."""
        self._log_progress_message("START: _convert_to_asset_metadata")
        md_list = []

        # Log some progress every 10 000 records. Without this output it is hard to see what is happening.
        progress_chunk_size = 10_000
        num_products = len(df)
        # Iterate using itertuples for lower overhead
        for i, row in enumerate(df.itertuples(index=False)):
            if i % progress_chunk_size == 0:
                fraction_done = i / num_products if num_products else 1
                gc.collect()
                self._log_progress_message(
                    f"Converted {i} of {num_products} to AssetMetadata ({fraction_done:.1%}). Memory usage: {psutil.Process().memory_info().rss // 1024**2:_}MB"
                )

            try:
                # Reconstruct BoundingBox lazily
                bbox_lat_lon = BoundingBox(
                    row.bbox_lat_lon_minx,
                    row.bbox_lat_lon_miny,
                    row.bbox_lat_lon_maxx,
                    row.bbox_lat_lon_maxy,
                    EPSG_4326_LATLON,
                )
                # Projected bbox only if proj differs
                if row.proj_epsg != EPSG_4326_LATLON:
                    try:
                        proj_bbox_vals = reproject_bounding_box(
                            row.bbox_lat_lon_minx,
                            row.bbox_lat_lon_miny,
                            row.bbox_lat_lon_maxx,
                            row.bbox_lat_lon_maxy,
                            from_crs=EPSG_4326_LATLON,
                            to_crs=row.proj_epsg,
                        )
                        bbox_projected = BoundingBox.from_list(proj_bbox_vals, row.proj_epsg)
                    except Exception:
                        _logger.debug("Reprojection failed for %s; using lat/lon bbox", row.asset_id, exc_info=True)
                        bbox_projected = bbox_lat_lon
                else:
                    bbox_projected = bbox_lat_lon

                kwargs = {
                    "href": row.href,
                    "original_href": row.original_href,
                    "asset_id": row.asset_id,
                    "item_id": row.item_id,
                    "tile_id": getattr(row, "tile_id", None),
                    "asset_type": getattr(row, "asset_type", None),
                    "media_type": getattr(row, "media_type", None),
                    "file_size": getattr(row, "file_size", None),
                    "proj_epsg": row.proj_epsg,
                    "bbox_lat_lon": bbox_lat_lon,
                    "geometry_lat_lon": row.geometry,
                    "bbox_projected": bbox_projected,
                    "datetime": row.datetime,
                    "start_datetime": row.start_datetime,
                    "end_datetime": row.end_datetime,
                }
                metadata = AssetMetadata(**{k: v for k, v in kwargs.items() if v is not None})
                md_list.append(metadata)
            except Exception:
                _logger.error(
                    "Failed to convert row %s to AssetMetadata", getattr(row, "asset_id", "<unknown>"), exc_info=True
                )

        self._log_progress_message(f"DONE: {i + 1} of {num_products} converted to AssetMetadata")
        self._log_progress_message("DONE: _convert_to_asset_metadata", level=logging.DEBUG)
        return md_list


def parse_tile_id(tile_id: str) -> Tuple[str, str]:
    """Parse the tile ID into its easting and northing components.

    The expected format is what we receive from OpenSearch.
    For example: "E09N27"
    """

    pos_N = tile_id.find("N")
    pos_S = tile_id.find("S")
    start_northing = None
    easting = None
    northing = None
    start_northing = max(pos_S, pos_N)

    if start_northing:
        easting = tile_id[:start_northing]
        northing = tile_id[start_northing:]

    return (northing, easting)


def display_collection_configs():
    """Just a small test function to show the CollectionConfig for each collection in HRL VPP"""
    collector = HRLVPPMetadataCollector()

    for coll in collector.get_tcc_collections():
        print(coll.id)
        pprint(coll.properties)
        print("-" * 50)

        conf_builder = CollectionConfigBuilder(coll)
        pprint(conf_builder.get_collection_config().model_dump())


if __name__ == "__main__":
    display_collection_configs()
