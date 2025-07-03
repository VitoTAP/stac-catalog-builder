from pathlib import Path
from typing import List, Optional

import terracatalogueclient as tcc
from pystac import Item

from stacbuilder import AssetMetadata, AssetMetadataPipeline, CollectionConfig
from stacbuilder.terracatalog import CollectionConfigBuilder, HRLVPPMetadataCollector


def vpp_list_metadata(
    collection_id: Optional[str] = None,
    max_products: Optional[int] = -1,
    query_by_frequency: str = "QS",
) -> List[AssetMetadata]:
    """Show the AssetMetadata objects that are generated for each VPP product.

    This is used to test the conversion and check the configuration files.
    """
    _check_tcc_collection_id(collection_id)
    collector = HRLVPPMetadataCollector(query_by_frequency=query_by_frequency)
    collector.collection_id = collection_id
    collector.max_products = max_products

    coll_cfg = collector.get_collection_config()
    pipeline = AssetMetadataPipeline.from_config(
        metadata_collector=collector,
        collection_config=coll_cfg,
        output_dir=None,
        overwrite=False,
    )
    return list(pipeline.get_metadata())


def vpp_list_stac_items(
    collection_id: Optional[str] = None,
    max_products: Optional[int] = -1,
    query_by_frequency: str = "QS",
) -> List[Item]:
    """Show the STAC items that are generated for each VPP product.

    This is used to test the conversion and check the configuration files.
    """
    _check_tcc_collection_id(collection_id)
    collector = HRLVPPMetadataCollector(query_by_frequency=query_by_frequency)
    collector.collection_id = collection_id
    collector.max_products = max_products

    coll_cfg = collector.get_collection_config()
    pipeline = AssetMetadataPipeline.from_config(
        metadata_collector=collector,
        collection_config=coll_cfg,
        output_dir=None,
        overwrite=False,
    )
    return list(pipeline.collect_stac_items())


def vpp_build_collection(
    collection_id: Optional[str] = None,
    output_dir: Optional[Path] = None,
    overwrite: Optional[bool] = False,
    max_products: Optional[int] = -1,
    query_by_frequency: str = "QS",
    item_postprocessor=None,
) -> None:
    """Build a STAC collection for one of the collections in HRL VPP (OpenSearch)."""

    _check_tcc_collection_id(collection_id)
    collector = HRLVPPMetadataCollector(temp_dir=output_dir, query_by_frequency=query_by_frequency)
    collector.collection_id = collection_id
    collector.max_products = max_products

    if output_dir and not isinstance(output_dir, Path):
        output_dir = Path(output_dir).expanduser().absolute()
        output_dir = output_dir / collection_id

    coll_cfg = collector.get_collection_config()
    pipeline: AssetMetadataPipeline = AssetMetadataPipeline.from_config(
        metadata_collector=collector,
        collection_config=coll_cfg,
        output_dir=output_dir,
        overwrite=overwrite,
        link_items=False,
    )
    if item_postprocessor:
        pipeline.item_postprocessor = item_postprocessor
    pipeline.build_collection()


def vpp_build_all_collections(
    output_dir: Path,
    overwrite: bool,
    max_products: Optional[int] = -1,
    query_by_frequency: str = "QS",
) -> None:
    """Build a STAC collection for each of the collections in HRL VPP (OpenSearch)."""

    collector = HRLVPPMetadataCollector(query_by_frequency=query_by_frequency)
    collector.max_products = max_products
    tcc_collections = collector.get_tcc_collections()

    coll: tcc.Collection
    for coll in tcc_collections:
        collector.collection_id = coll.id
        collector.collect()

        coll_cfg = collector.get_collection_config()
        pipeline = AssetMetadataPipeline.from_config(
            metadata_collector=collector,
            collection_config=coll_cfg,
            output_dir=output_dir,
            overwrite=overwrite,
        )

        pipeline.build_collection()


def _check_tcc_collection_id(collection_id: Optional[str]) -> str:
    """DEPRECATED Helper method to select the collection without dealing with long names"""
    if not collection_id:
        raise ValueError("No collection was specified. collection_id must have a non-empty string value.")
    if collection_id and not isinstance(collection_id, str):
        raise TypeError(f"Type of collection_id must be str. {type(collection_id)=}, {collection_id=!r}")
    collector = HRLVPPMetadataCollector()
    tcc_collections = collector.get_tcc_collections()

    if collection_id:
        if collection_id not in [c.id for c in tcc_collections]:
            raise ValueError(f'collection_id "{collection_id}" does not exists.')
        return collection_id


def vpp_get_tcc_collections() -> list[tcc.Collection]:
    """Display the CollectionConfig for each of the collections in HRL VPP."""
    collector = HRLVPPMetadataCollector()
    return list(collector.get_tcc_collections())


def vpp_count_products() -> list[tcc.Collection]:
    """Display the CollectionConfig for each of the collections in HRL VPP."""
    collector = HRLVPPMetadataCollector()
    catalogue = collector.get_tcc_catalogue()
    collections = list(collector.get_tcc_collections())
    return {c.id: catalogue.get_product_count(c.id) for c in collections}


def vpp_count_products_per_query_slot(collection_id: str) -> None:
    _check_tcc_collection_id(collection_id)
    collector = HRLVPPMetadataCollector()
    collector.collection_id = collection_id
    collector.list_num_prods_per_query_slot(collection_id)


def vpp_get_collection_config(collection_id: str) -> list[CollectionConfig]:
    """Display the CollectionConfig for each of the collections in HRL VPP."""
    _check_tcc_collection_id(collection_id)
    collector = HRLVPPMetadataCollector()
    collector.collection_id = collection_id
    tcc_coll = collector.get_tcc_collection()
    conf_builder = CollectionConfigBuilder(tcc_coll)
    return conf_builder.get_collection_config()


def vpp_get_all_collection_configs() -> list[CollectionConfig]:
    """Display the CollectionConfig for each of the collections in HRL VPP."""
    collector = HRLVPPMetadataCollector()

    configs = []
    for coll in collector.get_tcc_collections():
        conf_builder = CollectionConfigBuilder(coll)
        configs.append(conf_builder.get_collection_config())

    return configs
