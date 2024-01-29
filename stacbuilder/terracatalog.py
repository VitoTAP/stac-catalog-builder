"""
Support for extracting data from HRVPP.

This is done via the terracatalogueclient.

At present, all code in this module is still very experimental (d.d. 2024-01-29)

"""

import datetime as dt
from itertools import chain
from pprint import pprint
from typing import Any, Dict, List, Tuple


import geopandas as gpd
import pandas as pd
import pystac

import terracatalogueclient as tcc

# from terracatalogueclient import Catalogue, Collection, Product
from terracatalogueclient.config import CatalogueConfig
from terracatalogueclient.config import CatalogueEnvironment


from stacbuilder.metadata import AssetMetadata
from stacbuilder.boundingbox import BoundingBox


def show_collections(catalogue: tcc.Catalogue):
    # make sure to retrieve config for the HRVPP catalogue
    collections = list(catalogue.get_collections())
    for c in collections:
        print(f"{c.id} - {c.properties['title']}")

    col = collections[0]
    print(f"=== collection ID: {col.id} ===")
    pprint(col.properties)
    print("-" * 50)
    pprint(dir(col))
    print("=" * 50)
    print("=== geometry:  ===")
    pprint(col.geometry)
    print("=== geojson:  ===")
    pprint(col.geojson)
    print("=== bbox:  ===")
    pprint(col.bbox)
    print("=" * 50)


def collections_to_dataframe(catalogue: tcc.Catalogue) -> pd.DataFrame:
    catalogue = tcc.Catalogue()
    collections = list(catalogue.get_collections())
    for c in collections:
        print(f"{c.id} - {c.properties['title']}")

    # col = collections[0]
    # print(f"=== collection ID: {col.id} ===")
    # pprint(col.properties)
    # print("-" * 50)
    # pprint(dir(col))
    # print("=" * 50)
    # print(f"=== geometry:  ===")
    # pprint(col.geometry)
    # print(f"=== geojson:  ===")
    # pprint(col.geojson)
    # print(f"=== bbox:  ===")
    # pprint(col.bbox)
    # print("=" * 50)

    coll_records = [tuple(chain([c.id, c.bbox, c.geometry], c.properties.values())) for c in collections]
    columns = _get_columnNames(collections[0].properties)
    # columns = ["coll_id", "bbox", "properties"]
    df = pd.DataFrame.from_records(coll_records, columns=columns, index="coll_id")

    print("=== === ===")
    print(columns)
    print(coll_records[0])
    # for row in coll_records:
    #     print(row)

    return df


def _get_columnNames(properties: Dict[str, Any]) -> List[str]:
    cols = ["coll_id"]
    cols.append("bbox")
    cols.append("geometry")
    cols.extend(properties.keys())
    return cols


def create_stac_collections(catalogue: tcc.Catalogue) -> List[pystac.Collection]:
    collections = list(catalogue.get_collections())
    return [create_stac_collection(c) for c in collections]


def create_stac_collection(collection_info: tcc.Collection):
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


def count_products(catalogue, collection: tcc.Collection) -> pd.DataFrame:
    """counts how may products each time slot contains.

    Temporary function for exploring the data.
    TODO: remove count_products after initial development.
    """
    num_prods = catalogue.get_product_count(collection.id)
    pprint(f"product count for coll_id{collection.id}: {num_prods}")

    dt_start, dt_end = get_coll_temporal_extent(collection)
    dt_range_years = pd.date_range(dt_start, dt_end, freq="YS")
    dt_range_months = pd.date_range(dt_start, dt_end, freq="MS")

    pprint(dt_range_years)
    pprint(dt_range_months)

    slot_start = dt_range_months[0]
    for i, slot_start in enumerate(dt_range_months[:-1]):
        slot_end = dt_range_months[i + 1]

        count = catalogue.get_product_count(
            collection.id,
            start=slot_start,
            end=slot_end,
        )
        print(f"from {slot_start} to {slot_end}: num products: {count}")


# TODO: replace by function that creates/saves a dataframe.
def list_products(catalogue, collection: tcc.Collection):
    num_prods = catalogue.get_product_count(collection.id)
    pprint(f"product count for coll_id{collection.id}: {num_prods}")

    dt_start, dt_end = get_coll_temporal_extent(collection)
    dt_range_years = pd.date_range(dt_start, dt_end, freq="YS")
    dt_range_months = pd.date_range(dt_start, dt_end, freq="MS")

    pprint(dt_range_years)
    pprint(dt_range_months)

    slot_start = dt_range_months[0]
    for i, slot_start in enumerate(dt_range_months[:-1]):
        slot_end = dt_range_months[i + 1]

        count = catalogue.get_product_count(
            collection.id,
            start=slot_start,
            end=slot_end,
        )
        print(f"from {slot_start} to {slot_end}: num products: {count}")

        products = catalogue.get_products(collection.id, start=slot_start, end=slot_end, limit=1)
        for product in products:
            print(product.title)
            pprint(product.properties)

            stac_item = create_stac_item(product)
            pprint(stac_item.to_dict())

        # print(f"from {slot_start} to {slot_end}: num products: {len(list(products))}")
        # # for product in products:
        # #     print(product.title)

    # products = catalogue.get_products(
    #     collection.id,
    #     start=dt.date(2020, 5, 1),
    #     end=dt.date(2020, 6, 1),
    # )
    # for product in products:
    #     print(product.title)


# TODO: remove create_stac_item after initial development.
def create_stac_item(product: tcc.Product) -> pystac.Item:
    # TODO: to be removed, create AssetMetdata & reuse pipeline so it ingests that.
    data_links = product.properties.get("links", {}).get("data", [])
    href = data_links[0].get("href") if data_links else None
    title = product.title
    description = product.title
    product_type = product.properties["productInformation"]["productType"]

    item = pystac.Item(
        href=href,
        id=product.id,
        # title=product.title,
        # description=description,
        geometry=product.geometry,
        bbox=product.bbox,
        datetime=product.beginningDateTime,
        start_datetime=product.beginningDateTime,
        end_datetime=product.endingDateTime,
        properties={},
    )

    from pystac.extensions.item_assets import AssetDefinition

    asset_def = AssetDefinition(
        properties={
            "type": pystac.MediaType.GEOTIFF,
            "title": title,
            "description": description,
            # "eo:bands": bands,
            "roles": ["data"],
        }
    )
    asset: pystac.Asset = asset_def.create_asset(href)
    item.add_asset(product_type, asset)

    return item

    # terracatalogueclient.client.Product(id, title, geojson, geometry, bbox, beginningDateTime, endingDateTime, properties, data, related, previews, alternates)[source]

    # Product entry returned from a catalogue search.

    # Variables:
    #         id (str) – product identifier
    #         title (str) – product title
    #         geojson (dict) – GeoJSON representation of the product
    #         geometry (BaseGeometry) – product geometry as a Shapely geometry
    #         bbox (List[float]) – bounding box
    #         beginningDateTime (dt.datetime) – acquisition start date time
    #         endingDateTime (dt.datetime) – acquisition end date time
    #         properties (dict) – product properties
    #         data (List[ProductFile]) – product data files
    #         related (List[ProductFile]) – related resources (eg. cloud mask)
    #         previews (List[ProductFile]) – previews or quicklooks of the product
    #         alternates (List[ProductFile]) – metadata description in an alternative format


def get_products_as_dataframe(catalogue, collection: tcc.Collection) -> gpd.GeoDataFrame:
    num_prods = catalogue.get_product_count(collection.id)
    pprint(f"product count for coll_id{collection.id}: {num_prods}")

    dt_start, dt_end = get_coll_temporal_extent(collection)
    dt_range_years = pd.date_range(dt_start, dt_end, freq="YS")
    dt_range_months = pd.date_range(dt_start, dt_end, freq="MS")

    pprint(dt_range_years)
    pprint(dt_range_months)

    data_frames = []
    slot_start = dt_range_months[0]
    for i, slot_start in enumerate(dt_range_months[:-1]):
        slot_end = dt_range_months[i + 1]

        # count = catalogue.get_product_count(
        #     collection.id,
        #     start=slot_start,
        #     end=slot_end,
        # )
        # print(f"from {slot_start} to {slot_end}: num products: {count}")

        products = catalogue.get_products(collection.id, start=slot_start, end=slot_end)
        assets_md = []
        for p, product in enumerate(products):
            print("-" * 50)
            print(product.id)
            print(product.title)
            print("product properties:")
            pprint(product.properties)
            print("... end properties ...")

            asset_metadata = create_asset_metadata(product)
            assets_md.append(asset_metadata)
            pprint(asset_metadata.to_dict())

            asset_bbox: BoundingBox = asset_metadata.bbox_lat_lon

            print(f"{asset_bbox.as_polygon()=}")
            print(f"{product.geometry}")
            print(asset_bbox.as_polygon() == product.geometry)
            print("-" * 50)

            # Temporary break for testing, limit the testing dataset.
            if p > 10:
                break

        data = [{k: v for k, v in md.to_dict().items() if k != "geometry_lat_lon"} for md in assets_md]

        geoms = [md.geometry_lat_lon for md in assets_md]
        gdf = gpd.GeoDataFrame(data=data, crs=4326, geometry=geoms)
        gdf.index = gdf["asset_id"]
        data_frames.append(gdf)

        # temp_df = pd.DataFrame.from_records(md.to_dict() for md in assets_md)
        # data_frames.append(temp_df)

        # Temporary break for testing, limit the testing dataset.
        if i > 3:
            break

    return pd.concat(data_frames)


def create_asset_metadata(product: tcc.Product) -> pystac.Item:
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


def convert_to_stac_items(df: pd.DataFrame):
    for i in range(len(df)):
        record = df.iloc[i, :]
        metadata = AssetMetadata.from_geoseries(record)
        pprint(metadata.to_dict())


def main():
    config = CatalogueConfig.from_environment(CatalogueEnvironment.HRVPP)
    catalogue = tcc.Catalogue(config)

    # for stac_coll in create_stac_collections(catalogue):
    #     pprint(stac_coll.to_dict())

    collections = list(catalogue.get_collections())
    collection = collections[0]

    # count_products(catalogue, collection)
    # list_products(catalogue, collection)

    df_collection = get_products_as_dataframe(catalogue, collection)

    csv_file = f"df-coll_{collection.id}.pipe.csv"
    df_collection.to_csv(csv_file, sep="|")

    parquet_file = f"df-coll_{collection.id}.parquet"
    df_collection.to_parquet(parquet_file, index=True)

    # record = df_collection.iloc[0, :]
    # md = AssetMetadata.from_dict(record.to_dict())
    # pprint(md.to_dict())

    convert_to_stac_items(df_collection)


if __name__ == "__main__":
    main()
