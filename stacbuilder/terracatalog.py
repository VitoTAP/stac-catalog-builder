import datetime as dt
from itertools import chain
from pprint import pprint
from typing import Any, Dict, List, Tuple, Union, Optional


import pandas as pd
import pystac

import terracatalogueclient as tcc

# from terracatalogueclient import Catalogue, Collection, Product
from terracatalogueclient.config import CatalogueConfig
from terracatalogueclient.config import CatalogueEnvironment


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
    print(f"=== geometry:  ===")
    pprint(col.geometry)
    print(f"=== geojson:  ===")
    pprint(col.geojson)
    print(f"=== bbox:  ===")
    pprint(col.bbox)
    print("=" * 50)


def collections_to_dataframe(catalogue: tcc.Catalogue) -> pd.DataFrame:

    catalogue = Catalogue()
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


def list_products(catalogue, collection: tcc.Collection):
    num_prods = catalogue.get_product_count(collection.id)
    pprint(f"product count for coll_id{collection.id}: {num_prods}")

    dt_start, dt_end = get_coll_temporal_extent(collection)
    duration = dt_end - dt_start

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


def create_stac_item(product: tcc.Product) -> pystac.Item:

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


def main():
    config = CatalogueConfig.from_environment(CatalogueEnvironment.HRVPP)
    catalogue = tcc.Catalogue(config)

    for stac_coll in create_stac_collections(catalogue):
        pprint(stac_coll.to_dict())

    show_collections(catalogue)

    collections = list(catalogue.get_collections())
    coll = collections[0]
    pprint(dir(coll))

    list_products(catalogue, coll)

    # df = collections_to_dataframe(catalogue)

    # pprint(df)


if __name__ == "__main__":
    main()
