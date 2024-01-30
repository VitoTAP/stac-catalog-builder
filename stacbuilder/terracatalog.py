"""
Support for extracting data from HRVPP.

This is done via the terracatalogueclient.

At present, all code in this module is still very experimental (d.d. 2024-01-29)

"""

import datetime as dt
from pprint import pprint
from typing import List, Optional, Tuple


import geopandas as gpd
import pandas as pd
import pystac


import terracatalogueclient as tcc
from terracatalogueclient.config import CatalogueConfig
from terracatalogueclient.config import CatalogueEnvironment


from stacbuilder.metadata import AssetMetadata
from stacbuilder.boundingbox import BoundingBox
from stacbuilder.builder import IMetadataCollector


def create_stac_collection(collection_info: tcc.Collection):
    """
    First attempt to build an (empty) collection from the terracatalog Collection.
    Experimental and will be replaced.

    We need to get some of the properties for the STAC Collection from the terracatalog Collection.
    The missing once can still be filled in from a CollectionConfig.

    """
    props = collection_info.properties

    title = props.get("title") or collection_info.id
    description = props.get("abstract") or collection_info.id

    # Note: in the terracatalogue keyword is singular!
    keywords = collection_info.properties.get("keyword")

    dt_start, dt_end = get_coll_temporal_extent(collection_info)
    bbox = collection_info.bbox
    extent = pystac.Extent(
        pystac.SpatialExtent(bbox),
        pystac.TemporalExtent([[dt_start, dt_end]]),
    )

    stac_coll = pystac.Collection(
        id=collection_info.id,
        title=title,
        description=description,
        keywords=keywords,
        providers=None,
        extent=extent,
    )

    # item_assets_ext = ItemAssetsExtension.ext(collection, add_if_missing=True)
    # item_assets_ext.item_assets = self.get_item_assets_definitions()

    # RasterExtension.add_to(collection)
    # collection.stac_extensions.append(CLASSIFICATION_SCHEMA)

    return stac_coll


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


class HRLVPPMetadataCollector(IMetadataCollector):
    def __init__(self):
        super().__init__()
        self._df_products: Optional[gpd.GeoDataFrame] = None
        self._collection_id: Optional[str] = None
        self._max_products = 10

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
            return

        if not self._df_products:
            catalogue = self.get_tcc_catalogue()
            collection = self.get_tcc_collection()
            self._df_products = self.get_products_as_dataframe(
                catalogue=catalogue,
                collection=collection,
            )

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

    def get_products_as_dataframe(self, catalogue, collection: tcc.Collection) -> gpd.GeoDataFrame:
        num_prods = catalogue.get_product_count(collection.id)
        pprint(f"product count for coll_id{collection.id}: {num_prods}")

        dt_start, dt_end = get_coll_temporal_extent(collection)
        dt_range_months = pd.date_range(dt_start, dt_end, freq="MS")

        # pprint(dt_range_years)
        # pprint(dt_range_months)

        data_frames = []
        slot_start = dt_range_months[0]

        num_products_processed = 0
        for i, slot_start in enumerate(dt_range_months[:-1]):
            slot_end = dt_range_months[i + 1]

            products = catalogue.get_products(collection.id, start=slot_start, end=slot_end)
            assets_md = []
            for p, product in enumerate(products):
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
                # print(asset_bbox.as_polygon() == product.geometry)
                # print("-" * 50)

                num_products_processed += 1
                if self._max_products > 0 and num_products_processed > self._max_products:
                    break

            data = [{k: v for k, v in md.to_dict().items() if k != "geometry_lat_lon"} for md in assets_md]

            geoms = [md.geometry_lat_lon for md in assets_md]
            gdf = gpd.GeoDataFrame(data=data, crs=4326, geometry=geoms)
            gdf.index = gdf["asset_id"]
            data_frames.append(gdf)

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
        href = data_links[0].get("href") if data_links else None

        # Is this a better way to get the href?
        href2 = product.data[0].href
        assert href2 == href

        # In this case we should get the title and description from the source in
        # opensearch rather than our own collection config file
        # TODO: Add title + description to AssetMetadata, or some other intermediate for STAC items.
        product_type = props.get("productInformation", {}).get("productType")

        acquisitionInformation = props.get("acquisitionInformation")
        tile_id = None
        if acquisitionInformation:
            acquisitionInformation = acquisitionInformation[0]
            tile_id = acquisitionInformation.get("acquisitionParameters", {}).get("tileId")

        asset_metadata = AssetMetadata()
        asset_metadata.asset_id = product.id
        asset_metadata.asset_type = product_type
        asset_metadata.title = product.title

        # item_id is the asset_id without the product_type/band name at the end
        num_chars_to_remove = 1 + len(product_type)
        asset_metadata.item_id = asset_metadata.asset_id[:-num_chars_to_remove]

        asset_metadata.collection_id = props.get("parentIdentifier")
        asset_metadata.tile_id = tile_id

        asset_metadata.href = href
        asset_metadata.original_href = href
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


def main():
    collector = HRLVPPMetadataCollector()
    COLLECTION_ID = "copernicus_r_3035_x_m_hrvpp-st_p_2017-now_v01"
    collector.collection_id = COLLECTION_ID

    collector.collect()

    for md in collector.metadata:
        pprint(md.to_dict())


if __name__ == "__main__":
    main()
