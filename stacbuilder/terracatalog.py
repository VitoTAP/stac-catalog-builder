"""
Support for extracting data from HRVPP.

This is done via the terracatalogueclient.

At present, all code in this module is still very experimental (d.d. 2024-01-29)

"""

import datetime as dt
import inspect
import itertools
import logging
from pathlib import Path
from pprint import pprint
from typing import Any, Dict, List, Optional, Tuple
import concurrent.futures

# import resource
import psutil
import gc

import geopandas as gpd
import pandas as pd
from pystac.media_type import MediaType
from pystac.provider import ProviderRole

import terracatalogueclient as tcc
from terracatalogueclient.config import CatalogueConfig
from terracatalogueclient.config import CatalogueEnvironment
from terracatalogueclient import ProductFile

from stacbuilder.boundingbox import BoundingBox
from stacbuilder.collector import IMetadataCollector
from stacbuilder.config import AssetConfig, CollectionConfig, RasterBandConfig, ProviderModel
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
            asset_cfg = AssetConfig(
                title=title,
                description=title,
                media_type=media_type,
                raster_bands=[raster_cfg],
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


class HRLVPPMetadataCollector(IMetadataCollector):
    """Collects AssetMetadata for further processing for the HRL VPP collections from OpenSearch."""

    def __init__(self, temp_dir: Path | None = None, query_by_frequency: str | None = "QS"):
        super().__init__()

        # components: objects that we delegate work to.
        self._cfg_builder: CollectionConfigBuilder = None

        # state / collected information
        self._df_asset_metadata: Optional[gpd.GeoDataFrame] = None

        self._collection_id: Optional[str] = None
        self._max_products = -1
        self._query_by_frequency: str = query_by_frequency or "QS"

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

        if not self._df_asset_metadata:
            self._log_progress_message(
                "Downloading products to dataframe. Max products to retrieve: {self.max_products}"
            )
            self.get_products_as_dataframe()
            self._save_dataframes()

        _logger.info("PROGRESS: converting GeoDataFrame to list of AssetMetadata objects")
        self._metadata_list = self._convert_to_asset_metadata(self._df_asset_metadata)

        # Free up memory
        self._df_asset_metadata = None
        gc.collect()

        self._log_progress_message("DONE: collect")

    def _save_dataframes(self) -> None:
        if self.temp_dir:
            if not self.temp_dir.exists():
                self.temp_dir.mkdir(parents=True)

            if self._df_asset_metadata is not None:
                self._save_geodataframe(self._df_asset_metadata, f"asset_metadata-{self.collection_id}")

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

    def _store_asset_metadata_and_clear_memory(self, index: int):
        """Store the AssetMetadata objects to disk and clear the memory."""
        if self.temp_dir:
            self._log_progress_message("Storing AssetMetadata to disk", level=logging.INFO)
            if not (self.temp_dir / "intermediates").exists():
                (self.temp_dir / "intermediates").mkdir(parents=True, exist_ok=True)
            gdf_path = self._save_geodataframe(self._df_asset_metadata, self._get_intermediate_relative_path(index))
            self._df_asset_metadata = None
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
                if self._df_asset_metadata is None:
                    self._df_asset_metadata = gpd.read_parquet(path)
                else:
                    self._df_asset_metadata = pd.concat([self._df_asset_metadata, gpd.read_parquet(path)])
                    # self._df_asset_metadata = self._df_asset_metadata.drop_duplicates(subset=["asset_id"])
                self._log_progress_message(
                    f"Restored {path} from disk. Memory usage: {psutil.Process().memory_info().rss // 1024 ** 2:_}MB",
                    level=logging.DEBUG,
                )
            self._log_progress_message(
                f"Restored {len(self._df_asset_metadata):_} AssetMetadata products from disk.", level=logging.INFO
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
        """Collect the products / assets info from the terracatalogueclient, into a GeoDataframe,
        and save the GeoDataframe to disk.

        This allows us to retrieve all products first and then process them. This makes it easier
        to group the products that belong to one STAC item, because we don't have much control
        over what order we receive them.
        """
        self._log_progress_message("START: get_products_as_dataframe")

        self._df_asset_metadata = None

        # HACK parameters to split up calculation into smaller chunks
        slice_length = 100  # limits the active threads to prevent OOM errors
        min_chunk, max_chunk = 0, 1000  # limits the number of chunks we process

        catalogue = self.get_tcc_catalogue()
        collection = self.get_tcc_collection()
        num_products_processed = 0
        num_products_stored = 0
        product_ids = set()
        num_prods = catalogue.get_product_count(collection.id)
        max_prods_to_process = self._max_products if self._max_products > 0 else num_prods

        _logger.info(f"product count for coll_id {collection.id}: {num_prods}")

        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            query_slots = self._get_product_query_slots(frequency=self._query_by_frequency)

            limit_reached = False
            intermediate_paths = []

            for query_slots_iterator in range(0, len(query_slots), slice_length):
                if limit_reached:
                    break

                if not (min_chunk <= query_slots_iterator < max_chunk):
                    # This is a temporary measure to prevent OOM errors.
                    # We should find a better way to limit the number of products we process.
                    self._log_progress_message(
                        f"Skipping query slot {query_slots_iterator} as it is not in the range we want to process.",
                        level=logging.INFO,
                    )
                    continue

                if self._is_intermediate_stored(query_slots_iterator):
                    futures = []
                    num_products_processed = self._get_num_products_from_intermediate(query_slots_iterator)
                    intermediate_paths.append(
                        self.temp_dir
                        / self._get_intermediate_relative_path(query_slots_iterator).with_suffix(".parquet")
                    )
                    self._log_progress_message(
                        f"Slot {query_slots_iterator} is already stored. It conatains {num_products_processed:_} products."
                    )

                else:
                    query_slots_slice = query_slots[query_slots_iterator : query_slots_iterator + slice_length]
                    futures = (
                        executor.submit(self._fetch_timeslot, slot_start, slot_end, prod_type)
                        for (slot_start, slot_end), prod_type in query_slots_slice
                    )

                    for future in concurrent.futures.as_completed(futures):
                        new_products = future.result()
                        if not new_products:
                            # Avoid doing unnecessary work, might add empty dataframes to the total dataframe.
                            continue
                        new_products = [p for p in new_products if p.id not in product_ids]
                        self._log_progress_message(f"Number of new products {len(new_products)}", level=logging.DEBUG)
                        product_ids.update([p.id for p in new_products])
                        self._log_progress_message(f"Number of unique products {len(product_ids)}", level=logging.DEBUG)

                        self._add_items_to_gdf(new_products)
                        del future

                        self._log_progress_message(
                            f"Number of new products {len(self._df_asset_metadata)-num_products_processed}.",
                            level=logging.DEBUG,
                        )

                        num_products_processed = len(self._df_asset_metadata)
                        percent_processed = (num_products_processed + num_products_stored) / max_prods_to_process
                        self._log_progress_message(
                            f"Progress: {num_products_processed + num_products_stored} of {max_prods_to_process} ({percent_processed:.1%}) Memory usage: {psutil.Process().memory_info().rss // 1024 ** 2:_}MB"
                        )  # {resource.getrusage(resource.RUSAGE_SELF).ru_maxrss:_}
                        if num_products_processed > max_prods_to_process:
                            executor.shutdown(wait=False)
                            limit_reached = True
                            break
                    intermediate_paths.append(self._store_asset_metadata_and_clear_memory(query_slots_iterator))
                num_products_stored += num_products_processed
                del futures
                gc.collect()

        self._restore_asset_metadata_from_disk(intermediate_paths)
        # Verify we have no duplicate products,
        # i.e. the number of unique product IDs must be == to the number of products.
        self._log_progress_message("START sanity checks: no duplicate products present and received all products ...")
        product_ids = set(self._df_asset_metadata.index)

        if len(product_ids) != len(self._df_asset_metadata):
            # raise DataValidationError(
            #     "Sanity check failed in get_products_as_dataframe:"
            #     + " Each products should correspond to exactly 1 AssetMetadata instance."
            #     + " len(product_ids) != len(self._df_asset_metadata)"
            #     + f" {len(product_ids)=} {len(self._df_asset_metadata)=}"
            # )
            self._log_progress_message(
                "Sanity check failed in get_products_as_dataframe:"
                + " Each products should correspond to exactly 1 AssetMetadata instance."
                + " len(product_ids) != len(self._df_asset_metadata)"
                + f" {len(product_ids)=} {len(self._df_asset_metadata)=}",
                level=logging.ERROR,
            )

        # Check that we have processed all products, based on the product count reported by the terracatalogueclient.
        if not self.max_products:
            if len(product_ids) != num_prods:
                # raise DataValidationError(
                #     "Sanity check failed in get_products_as_dataframe:"
                #     + "Number of products in result must be the product count reported by terracataloguiclient"
                #     + " len(product_ids) != num_prods"
                #     + f" {len(product_ids)=} product count: {num_prods=}"
                # )
                self._log_progress_message(
                    "Sanity check failed in get_products_as_dataframe:"
                    + "Number of products in result must be the product count reported by terracataloguiclient"
                    + " len(product_ids) != num_prods"
                    + f" {len(product_ids)=} product count: {num_prods=}",
                    level=logging.ERROR,
                )
        self._log_progress_message("DONE sanity checks")

        self._log_progress_message("DONE: get_products_as_dataframe", level=logging.DEBUG)
        return self._df_asset_metadata

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
        self._log_progress_message("START: adding new assets to AssetMetadata GeoDataFrame ...", level=logging.DEBUG)
        assets_md = [self.create_asset_metadata(p) for p in new_products]
        asset_records = [{k: v for k, v in md.to_dict().items() if k != "geometry_lat_lon"} for md in assets_md]
        asset_geoms = [md.geometry_lat_lon for md in assets_md]
        gdf_asset_md = gpd.GeoDataFrame(data=asset_records, crs=EPSG_4326_LATLON, geometry=asset_geoms)
        gdf_asset_md.index = gdf_asset_md["asset_id"]
        gdf_asset_md.sort_index()

        if self._df_asset_metadata is None:
            self._df_asset_metadata = gdf_asset_md
        else:
            self._df_asset_metadata = pd.concat([self._df_asset_metadata, gdf_asset_md])
            self._df_asset_metadata = self._df_asset_metadata.drop_duplicates(subset=["asset_id"])

        self._log_progress_message("DONE: adding new assets to AssetMetadata GeoDataFrame", level=logging.DEBUG)

    def _product_to_dict(self, product: tcc.Product) -> dict[str, Any]:
        return {
            "id": product.id,
            "title": product.title,
            "geojson": product.geojson,
            "geometry": product.geometry,
            "bbox": product.bbox,
            "beginningDateTime": product.beginningDateTime,
            "endingDateTime": product.endingDateTime,
            "properties": product.properties,
            "data": [self._product_file_to_dict(f) for f in product.data],
            "related": [self._product_file_to_dict(f) for f in product.related],
            "previews": [self._product_file_to_dict(f) for f in product.previews],
        }

    def _product_file_to_dict(self, product_file: ProductFile) -> dict[str, Any]:
        return {
            "href": product_file.href,
            "length": product_file.length,
            "title": product_file.title,
            "type": product_file.type,
            "category": product_file.category,
        }

    def _convert_to_asset_metadata(self, df: pd.DataFrame) -> List[AssetMetadata]:
        """Convert the pandas dataframe to a list of AssetMetadata objects."""
        self._log_progress_message("START: _convert_to_asset_metadata")
        md_list = []

        # Log some progress every 10 000 records. Without this output it is hard to see what is happening.
        progress_chunk_size = 10_000
        num_products = len(df)
        for i in range(num_products):
            if i % progress_chunk_size == 0:
                fraction_done = i / num_products
                self._log_progress_message(f"Converted {i} of {num_products} to AssetMetadata ({fraction_done:.1%})")

            record = df.iloc[i, :]
            metadata = AssetMetadata.from_geoseries(record)
            md_list.append(metadata)

        self._log_progress_message(f"DONE: {i+1} of {num_products} converted to AssetMetadata")
        self._log_progress_message("DONE: _convert_to_asset_metadata", level=logging.DEBUG)
        return md_list

    def create_asset_metadata(self, product: tcc.Product) -> AssetMetadata:
        """Create a AssetMetadata object containing all relevant info from the product in OpenSearch/terracatalogueclient"""
        props = product.properties
        data_links = props.get("links", {}).get("data", [])
        # TODO: it seems we can have multiple links for multiple assets here: how to handle them?
        #   Is each link a separate asset in STAC terms?
        #   And does our data even use this or does the list only ever contain one element?
        first_link = data_links[0]
        first_prod_data = product.data[0] if product.data else None

        href = first_prod_data.href
        # TODO: remove temporary assert for development. The above method to get href seems better but want to verify that they are indeed identical.
        href2 = first_link.get("href") if data_links else None
        assert href2 == href

        asset_metadata = AssetMetadata()
        asset_metadata.href = href
        asset_metadata.original_href = href
        asset_metadata.asset_id = product.id
        asset_metadata.collection_id = props.get("parentIdentifier")
        asset_metadata.title = product.title

        # product type is a shorter code than what corresponds to asset_metadata.asset_type
        # The product type is more general. But the OpenSearch title (asset_type for is) appends the spatial resolution.
        # For example: product_type="PPI", title="PPI_10M"
        product_type = props.get("productInformation", {}).get("productType")
        # item_id is the asset_id without the product_type/band name at the end
        num_chars_to_remove = 1 + len(product_type)
        asset_metadata.item_id = asset_metadata.asset_id[:-num_chars_to_remove]

        # In this case we should get the title and description from the source in
        # OpenSearch rather than our own collection config file
        # TODO: Add title + description to AssetMetadata, or some other intermediate for STAC items.
        acquisitionInformation = props.get("acquisitionInformation")
        tile_id = None
        if acquisitionInformation:
            acquisitionInformation = acquisitionInformation[0]
            tile_id = acquisitionInformation.get("acquisitionParameters", {}).get("tileId")
            asset_metadata.tile_id = tile_id

        if product.data:
            first_prod_data = product.data[0]
            asset_metadata.asset_type = first_prod_data.title
            asset_metadata.media_type = AssetMetadata.mime_to_media_type(first_prod_data.type)
            asset_metadata.file_size = first_prod_data.length

        epsg_code = None
        if data_links:
            # example:
            #   conformsTo': 'http://www.opengis.net/def/crs/EPSG/0/3035'
            epsg_url = first_link.get("conformsTo")
            parts_epsg_code = epsg_url.split("/")
            if "EPSG" in parts_epsg_code:
                try:
                    epsg_code = int(parts_epsg_code[-1])
                except Exception:
                    _logger.error(
                        f"Could not get EPSG code for product with ID={product.id}, {epsg_url=}", exc_info=True
                    )
                    raise
        asset_metadata.proj_epsg = epsg_code or EPSG_4326_LATLON

        asset_metadata.bbox_lat_lon = BoundingBox.from_list(product.bbox, epsg=EPSG_4326_LATLON)
        asset_metadata.geometry_lat_lon = product.geometry
        if epsg_code:
            proj_bbox = reproject_bounding_box(*product.bbox, from_crs=EPSG_4326_LATLON, to_crs=epsg_code)
            asset_metadata.bbox_projected = BoundingBox.from_list(proj_bbox, epsg_code)
        else:
            asset_metadata.bbox_projected = asset_metadata.bbox_lat_lon

        asset_metadata.datetime = product.beginningDateTime
        asset_metadata.start_datetime = product.beginningDateTime
        asset_metadata.end_datetime = product.endingDateTime

        # TODO: should we also process the following attributes of product?
        #   - product.alternates
        #   - product.geojson
        #   - product.geometry
        #   - product.previews
        #   - product.related
        #   - product.properties

        # TODO: should we also process the following keys in the product.properties dict?
        # 'acquisitionInformation': [{'acquisitionParameters': {'acquisitionType': 'NOMINAL', 'beginningDateTime': '2017-04-01T00:00:00.000Z', 'endingDateTime': '2017-04-01T23:59:59.999Z', 'tileId': 'E09N27'},
        #                             'platform': {'platformSerialIdentifier': 'S2A, S2B', 'platformShortName': 'SENTINEL-2'}}],
        # 'additionalAttributes': {'resolution': 10},
        # 'available': '2022-10-17T15:16:33Z',
        # 'date': '2017-04-01T00:00:00.000Z',
        # 'identifier': 'ST_20170401T000000_S2_E09N27-03035-010m_V101_PPI',
        # 'links': {'alternates': [],
        #         'data': [{'conformsTo': 'http://www.opengis.net/def/crs/EPSG/0/3035',
        #                     'href': 'https://phenology.vgt.vito.be/download/ST_LAEA_V01/2017/CLMS/Pan-European/Biophysical/ST_LAEA/v01/2017/04/01/ST_20170401T000000_S2_E09N27-03035-010m_V101_PPI.tif',
        #                     'length': 3264953,
        #                     'title': 'PPI_10M',
        #                     'type': 'image/tiff'}],
        #         'previews': [],
        #         'related': []},
        # 'parentIdentifier': 'copernicus_r_3035_x_m_hrvpp-st_p_2017-now_v01',
        # 'productInformation': {'availabilityTime': '2022-10-17T15:16:33Z', 'processingCenter': 'VITO', 'processingDate': '2021-08-23T15:07:52.759Z', 'productType': 'PPI', 'productVersion': 'V101'},
        # 'published': '2022-10-17T15:16:33Z',
        # 'status': 'ARCHIVED',
        # 'title': 'Seasonal Trajectories 2017-now (raster 010m) - version 1 revision 01 : PPI E09N27 20170401T000000',
        # 'updated': '2021-08-23T15:07:52.759Z'}
        return asset_metadata


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
