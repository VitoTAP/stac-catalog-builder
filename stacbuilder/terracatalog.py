"""
Support for extracting data from HRVPP.

This is done via the terracatalogueclient.

At present, all code in this module is still very experimental (d.d. 2024-01-29)

"""

import datetime as dt
import logging
from pprint import pprint
from typing import Any, Dict, List, Optional, Tuple


import geopandas as gpd
import pandas as pd
from pystac.media_type import MediaType
from pystac.provider import ProviderRole

import terracatalogueclient as tcc
from terracatalogueclient.config import CatalogueConfig
from terracatalogueclient.config import CatalogueEnvironment


from stacbuilder.metadata import AssetMetadata
from stacbuilder.boundingbox import BoundingBox
from stacbuilder.collector import IMetadataCollector

from stacbuilder.config import AssetConfig, CollectionConfig, RasterBandConfig, ProviderModel


_logger = logging.getLogger(__name__)


def get_coll_temporal_extent(collection: tcc.Collection) -> Tuple[dt.datetime | None, dt.datetime | None]:
    acquisitionInformation = collection.properties["acquisitionInformation"]
    pprint(acquisitionInformation)
    for info in acquisitionInformation:
        print(info.get("acquisitionParameters", {}))

        dt_start = info.get("acquisitionParameters", {}).get("beginningDateTime")
        dt_end = info.get("acquisitionParameters", {}).get("endingDateTime")

        print(dt_start, dt_end)
        print(dt.datetime.fromisoformat(dt_start))
        print(dt.datetime.fromisoformat(dt_end))

        dt_start = dt.datetime.fromisoformat(dt_start)
        dt_end = dt.datetime.fromisoformat(dt_end)

    return dt_start, dt_end


class CollectionConfigBuilder:
    def __init__(self, tcc_collection: tcc.Collection):
        self.tcc_collection = tcc_collection

    def get_collection_config(self) -> CollectionConfig:
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
        platforms = []
        acq_info = self.get_acquisition_info()
        for info in acq_info:
            platform = info.get("platform")
            if platform:
                platforms.append(platform.get("platformShortName"))

        return list(set(platforms))

    def get_instruments(self) -> Optional[List[str]]:
        instruments = []
        acq_info = self.get_acquisition_info()
        for info in acq_info:
            instrument = info.get("instrument", {})
            if instrument:
                instruments.append(instrument.get("instrumentShortName"))

        return list(set(instruments))

    def get_acquisition_info(self) -> Dict[str, Any]:
        return self.tcc_collection.properties.get("acquisitionInformation", [])

    def get_product_info(self) -> Dict[str, Any]:
        return self.tcc_collection.properties.get("productInformation", {})

    def get_format(self) -> Optional[str]:
        prod_info = self.get_product_info()
        return prod_info.get("format")

    def get_asset_config(self) -> List[AssetConfig]:
        media_type = self.get_media_type()

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
        # TODO: need to add setting so we know if we should assume it is an int type or a a float type
        #   While float types with less than 32 bits doe exist, they are not common.
        #   Not sure if EO ever uses those, but they do exist in other industries
        #   (for example it is commonly used in the EXR image format, but I haven't heard of any other examples)
        #   So for 16 bits and less we could assume it must be an int.
        #    For 32 and 64 bit it could be either float or int.
        return f"uint{bit_per_value}"

    def get_media_type(self) -> MediaType:
        format = self.get_format()
        media_type = None
        if format.lower() in ["geotif", "geotiff", "tiff"]:
            media_type = MediaType.GEOTIFF
        elif format.lower() == "COG":
            media_type = MediaType.COG
        return media_type

    def get_product_types(self):
        return self.get_product_info().get("productType")


class HRLVPPMetadataCollector(IMetadataCollector):
    def __init__(self):
        super().__init__()

        # components: objects that we delegate work to.
        self._cfg_builder: CollectionConfigBuilder = None

        # state / collected information
        self._df_products: Optional[gpd.GeoDataFrame] = None
        self._collection_id: Optional[str] = None
        self._max_products = -1

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
        return self._max_products

    @max_products.setter
    def max_products(self, value: int) -> None:
        if not isinstance(value, int):
            raise TypeError(f"Value for max_products must be an int. {type(value)=}, {value=}")
        self._max_products = int(value) if value else -1

    def collect(self):
        if self.has_collected():
            _logger.info("Already collected data. Returning")
            return

        if not self._df_products:
            _logger.debug(f"Downloading products to dataframe. Max products to retrieve: {self.max_products}")
            self._df_products = self.get_products_as_dataframe()

        self._metadata_list = self._convert_to_asset_metadata(self._df_products)

    def get_tcc_catalogue(self) -> tcc.Catalogue:
        config = CatalogueConfig.from_environment(CatalogueEnvironment.HRVPP)
        return tcc.Catalogue(config)

    def get_tcc_collections(self) -> List[tcc.Collection]:
        catalogue = self.get_tcc_catalogue()
        return list(catalogue.get_collections())

    def get_tcc_collection(self) -> tcc.Collection:
        for coll in self.get_tcc_collections():
            if coll.id == self.collection_id:
                return coll
        return None

    def get_collection_config(self) -> CollectionConfig:
        tcc_collection = self.get_tcc_collection()
        if not tcc_collection:
            return None

        self._cfg_builder = CollectionConfigBuilder(self.get_tcc_collection())
        return self._cfg_builder.get_collection_config()

    def get_products_as_dataframe(self) -> gpd.GeoDataFrame:
        catalogue = self.get_tcc_catalogue()
        collection = self.get_tcc_collection()
        num_prods = catalogue.get_product_count(collection.id)
        pprint(f"product count for coll_id {collection.id}: {num_prods}")

        # We retrieve the products in chunks per day, and per product type (see below)
        # So divide the temporal extent into slots per day:
        dt_start, dt_end = get_coll_temporal_extent(collection)
        dt_ranges = pd.date_range(dt_start, dt_end, freq="D")
        pprint(dt_ranges)

        data_frames = []
        slot_start = dt_ranges[0]
        num_products_processed = 0

        for i, slot_start in enumerate(dt_ranges[:-1]):
            slot_end = dt_ranges[i + 1]
            prod_types = self._cfg_builder.get_product_types()

            # Sometimes getting the data per data still hits the limit of how many
            # products you can retrieve in one go.
            # Therefore, dividing it up into product types as well.
            for prod_type in prod_types:
                products = list(
                    catalogue.get_products(collection.id, start=slot_start, end=slot_end, productType=prod_type)
                )
                assets_md = []
                if not products:
                    # There is no data for this time range.
                    continue

                for product in products:
                    # Apply our own max_products limit for testing & troubleshooting:
                    # in the inner-most loop
                    num_products_processed += 1
                    if self._max_products > 0 and num_products_processed > self._max_products:
                        break

                    # print("-" * 50)
                    # print(product.id)
                    # print(product.title)
                    # print("product properties:")
                    # pprint(product.properties)
                    # print("... end properties ...")

                    asset_metadata = self.create_asset_metadata(product)
                    assets_md.append(asset_metadata)
                    # pprint(asset_metadata.to_dict())

                    # asset_bbox: BoundingBox = asset_metadata.bbox_lat_lon
                    # print(f"{asset_bbox.as_polygon()=}")
                    # print(f"{product.geometry}")
                    # print("-" * 50)

                # The extra break statements are needed so we don't end up with
                # an empty dataframe here, which is something we cannot process.
                data = [{k: v for k, v in md.to_dict().items() if k != "geometry_lat_lon"} for md in assets_md]
                geoms = [md.geometry_lat_lon for md in assets_md]
                gdf = gpd.GeoDataFrame(data=data, crs=4326, geometry=geoms)
                gdf.index = gdf["asset_id"]
                data_frames.append(gdf)

                # Apply our own max_products limit for testing & troubleshooting:
                #   Break out of the second loop.
                if self._max_products > 0 and num_products_processed > self._max_products:
                    break

            # Apply our own max_products limit for testing & troubleshooting:
            #   Break out of the outer loop.
            if self._max_products > 0 and num_products_processed > self._max_products:
                break

        return pd.concat(data_frames)

    @staticmethod
    def _convert_to_asset_metadata(df: pd.DataFrame):
        md_list = []
        for i in range(len(df)):
            record = df.iloc[i, :]
            metadata = AssetMetadata.from_geoseries(record)
            md_list.append(metadata)
        return md_list

    def create_asset_metadata(self, product: tcc.Product) -> AssetMetadata:
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
            # conformsTo': 'http://www.opengis.net/def/crs/EPSG/0/3035'
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
        asset_metadata.proj_epsg = epsg_code

        # if data_links:
        #     asset_metadata.asset_type = first_link.get("title")
        #     file_type = first_link.get("type")
        #     asset_metadata.media_type = AssetMetadata.mime_to_media_type(file_type)
        #     asset_metadata.file_size = first_link.get("length")

        asset_metadata.bbox_lat_lon = BoundingBox.from_list(product.bbox, epsg=4326)
        asset_metadata.geometry_lat_lon = product.geometry

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


def main():
    collector = HRLVPPMetadataCollector()

    for coll in collector.get_tcc_collections():
        print(coll.id)
        pprint(coll.properties)
        print("-" * 50)

        conf_builder = CollectionConfigBuilder(coll)
        pprint(conf_builder.get_collection_config().model_dump())


if __name__ == "__main__":
    main()
