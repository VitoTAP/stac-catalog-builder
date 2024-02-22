"""
CLI command-style functions

The functions below are helper functions to keep the CLI in __main__.py
as thin and dumb as reasonably possible.

We want to the logic out of the CLI, therefore we put it in these functions
and the CLI only does the argument parsing, via the click library.

The main advantage is that this style allows for unit tests on core
functionality of the CLI, and that is harder to do directly on the CLI.
"""

from pathlib import Path
from typing import List, Optional, Tuple


from pystac import Collection, Item
import terracatalogueclient as tcc
from stacbuilder.metadata import GeodataframeExporter
from stacbuilder.collector import FileCollector


from stacbuilder.builder import (
    AssetMetadataPipeline,
    GeoTiffPipeline,
    PostProcessSTACCollectionFile,
)
from stacbuilder.config import CollectionConfig, FileCollectorConfig
from stacbuilder.metadata import AssetMetadata
from stacbuilder.terracatalog import HRLVPPMetadataCollector, CollectionConfigBuilder
from stacbuilder.stacapi.upload import Uploader


def build_collection(
    collection_config_path: Path,
    glob: str,
    input_dir: Path,
    output_dir: Path,
    overwrite: bool,
    max_files: Optional[int] = -1,
    save_dataframe: Optional[bool] = False,
) -> None:
    """
    Build a STAC collection from a directory of files.

    :param collection_config_path: Path to the collection configuration file.
    :param glob: Glob pattern to match the files within the input_dir.
    :param input_dir: Root directory where the files are located.
    :param output_dir: Directory where the STAC collection will be saved.
    :param overwrite: Overwrite the output directory if it exists.
    :param max_files: Maximum number of files to process.
    :param save_dataframe: Save the geodataframe of the STAC items.
    """
    collection_config_path = Path(collection_config_path).expanduser().absolute()
    coll_cfg = CollectionConfig.from_json_file(collection_config_path)
    file_coll_cfg = FileCollectorConfig(input_dir=input_dir, glob=glob, max_files=max_files)

    if output_dir and not isinstance(output_dir, Path):
        output_dir = Path(output_dir).expanduser().absolute()

    pipeline = GeoTiffPipeline.from_config(
        collection_config=coll_cfg,
        file_coll_cfg=file_coll_cfg,
        output_dir=output_dir,
        overwrite=overwrite,
    )

    pipeline.build_collection()

    if save_dataframe:
        GeodataframeExporter.export_item_bboxes(pipeline.collection)


def build_grouped_collections(
    collection_config_path: Path,
    glob: str,
    input_dir: Path,
    output_dir: Path,
    overwrite: bool,
    max_files: Optional[int] = -1,
    save_dataframe: Optional[bool] = False,
) -> None:
    """
    Build a multiple STAC collections from a directory of files,
    where each collection groups related STAC items.

    The default grouping is per year.

    :param collection_config_path: Path to the collection configuration file.
    :param glob: Glob pattern to match the files within the input_dir.
    :param input_dir: Root directory where the files are located.
    :param output_dir: Directory where the STAC collection will be saved.
    :param overwrite: Overwrite the output directory if it exists.
    :param max_files: Maximum number of files to process.
    :param save_dataframe: Save the geodataframe of the STAC items.
    """

    collection_config_path = Path(collection_config_path).expanduser().absolute()
    coll_cfg = CollectionConfig.from_json_file(collection_config_path)
    file_coll_cfg = FileCollectorConfig(input_dir=input_dir, glob=glob, max_files=max_files)

    if output_dir and not isinstance(output_dir, Path):
        output_dir = Path(output_dir).expanduser().absolute()

    pipeline = GeoTiffPipeline.from_config(
        collection_config=coll_cfg,
        file_coll_cfg=file_coll_cfg,
        output_dir=output_dir,
        overwrite=overwrite,
    )

    pipeline.build_grouped_collections()

    if save_dataframe:
        for collection in pipeline.collection_groups.values():
            GeodataframeExporter.export_item_bboxes(collection)


def extract_item_bboxes(collection_file: Path):
    """Extract the bounding boxes of the STAC items in the collection."""
    collection = Collection.from_file(collection_file)
    GeodataframeExporter.export_item_bboxes(collection)


def list_input_files(
    glob: str,
    input_dir: Path,
    max_files: Optional[int] = -1,
) -> list[Path]:
    """
    Searches the files that are found with the current configuration.

    This can be used to test the glob pattern and the input directory.

    :param glob: Glob pattern to match the files within the input_dir.
    :param input_dir: Root directory where the files are located.
    :param max_files: Maximum number of files to process.
    :return: List containing paths of all the found files.
    """

    collector = FileCollector(
        input_dir=Path(input_dir),
        glob=glob,
        max_files=max_files,
    )
    collector.collect()
    return collector.input_files


def list_asset_metadata(
    collection_config_path: Path,
    glob: str,
    input_dir: Path,
    max_files: Optional[int] = -1,
    save_dataframe: bool = False,
) -> List[AssetMetadata]:
    """
    Return the AssetMetadata objects generated for each file.

    This is used to test the digestion of input files and check the configuration files.

    :param collection_config_path: Path to the collection configuration file.
    :param glob: Glob pattern to match the files within the input_dir.
    :param input_dir: Root directory where the files are located.
    :param max_files: Maximum number of files to process.
    :param save_dataframe: Save the geodataframe of the metadata.
    :return: List of AssetMetadata objects for each file.
    """

    collection_config_path = Path(collection_config_path).expanduser().absolute()
    coll_cfg = CollectionConfig.from_json_file(collection_config_path)
    file_coll_cfg = FileCollectorConfig(input_dir=input_dir, glob=glob, max_files=max_files)
    pipeline = GeoTiffPipeline.from_config(collection_config=coll_cfg, file_coll_cfg=file_coll_cfg)

    if save_dataframe:
        df = pipeline.get_metadata_as_geodataframe()
        # TODO: Want a better directory to save geodata, maybe use save_dataframe as path instead of flag.
        out_dir = Path("tmp") / coll_cfg.collection_id / "visualization_list-assetmetadata"
        GeodataframeExporter.save_geodataframe(df, out_dir, "metadata_table")

    return pipeline.get_asset_metadata()


def list_stac_items(
    collection_config_path: Path,
    glob: str,
    input_dir: Path,
    max_files: Optional[int] = -1,
    save_dataframe: bool = False,
) -> Tuple[List[Collection], List[Path]]:
    """
    Return the STAC items that are generated for each file and the files for which no stac item could be generated.

    This is used to test the creation of individual stac items from files.

    :param collection_config_path: Path to the collection configuration file.
    :param glob: Glob pattern to match the files within the input_dir.
    :param input_dir: Root directory where the files are located.
    :param max_files: Maximum number of files to process.
    :param save_dataframe: Save the geodataframe of the STAC items.
    :return: Tuple containing a List of STAC items and a list of files for which no item could be generated.
    """

    collection_config_path = Path(collection_config_path).expanduser().absolute()
    coll_cfg = CollectionConfig.from_json_file(collection_config_path)
    file_coll_cfg = FileCollectorConfig(input_dir=input_dir, glob=glob, max_files=max_files)
    pipeline = GeoTiffPipeline.from_config(
        collection_config=coll_cfg, file_coll_cfg=file_coll_cfg, output_dir=None, overwrite=False
    )

    if save_dataframe:
        df = pipeline.get_stac_items_as_geodataframe()
        # TODO: Want better directory to save geodata, maybe use save_dataframe as path instead of flag.
        out_dir = Path("tmp") / coll_cfg.collection_id / "visualization_list-stac-items"
        GeodataframeExporter.save_geodataframe(df, out_dir, "stac_items")

    # TODO: how to deal with item grouping for the grouped collections in this command?
    #   Maybe just don't show the groups here and add a separate command to show them with grouping.
    stac_items = list(pipeline.collect_stac_items())
    files = list(pipeline.get_input_files())
    failed_files = [files[i] for i, item in enumerate(stac_items) if item is None]

    return stac_items, failed_files


def postprocess_collection(
    collection_file: Path,
    collection_config_path: Path,
    output_dir: Optional[Path] = None,
) -> None:
    """
    Run only the post-processing step, on an existing STAC collection.

    Mainly intended to troubleshoot the postprocessing so you don't have to
    regenerate the entire set every time.

    :param collection_file: Path to the STAC collection file.
    :param collection_config_path: Path to the collection configuration file.
    :param output_dir: Directory where the STAC collection will be saved.
    """
    collection_config_path = Path(collection_config_path).expanduser().absolute()
    coll_cfg = CollectionConfig.from_json_file(collection_config_path)

    postprocessor = PostProcessSTACCollectionFile(collection_overrides=coll_cfg.overrides)
    postprocessor.process_collection(collection_file=collection_file, output_dir=output_dir)


def load_collection(
    collection_file: Path,
) -> Collection:
    """
    Load and return the STAC collection in 'collection_file'.

    :param collection_file: Path to the STAC collection file.
    :return: The STAC collection object.
    """
    return Collection.from_file(collection_file)


def validate_collection(
    collection_file: Path,
) -> None:
    """
    Validate a STAC collection.

    :param collection_file: Path to the STAC collection file.
    """
    collection = Collection.from_file(collection_file)
    collection.validate_all()


def vpp_list_metadata(
    collection_id: Optional[str] = None,
    max_products: Optional[int] = -1,
) -> List[AssetMetadata]:
    """Show the AssetMetadata objects that are generated for each VPP product.

    This is used to test the conversion and check the configuration files.
    """
    _check_tcc_collection_id(collection_id)
    collector = HRLVPPMetadataCollector()
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
) -> List[Item]:
    """Show the STAC items that are generated for each VPP product.

    This is used to test the conversion and check the configuration files.
    """
    _check_tcc_collection_id(collection_id)
    collector = HRLVPPMetadataCollector()
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
) -> None:
    """Build a STAC collection for one of the collections in HRL VPP (OpenSearch)."""

    _check_tcc_collection_id(collection_id)
    collector = HRLVPPMetadataCollector(temp_dir=output_dir)
    collector.collection_id = collection_id
    collector.max_products = max_products

    if output_dir and not isinstance(output_dir, Path):
        output_dir = Path(output_dir).expanduser().absolute()
        output_dir = output_dir / collection_id

    coll_cfg = collector.get_collection_config()
    pipeline = AssetMetadataPipeline.from_config(
        metadata_collector=collector,
        collection_config=coll_cfg,
        output_dir=output_dir,
        overwrite=overwrite,
    )
    pipeline.build_collection(link_items=False)


def vpp_build_all_collections(
    output_dir: Path,
    overwrite: bool,
    max_products: Optional[int] = -1,
    # save_dataframe: Optional[bool] = False,
) -> None:
    """Build a STAC collection for each of the collections in HRL VPP (OpenSearch)."""

    collector = HRLVPPMetadataCollector()
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


def upload_to_stac_api(collection_path: Path) -> None:
    """Upload a collection to the STAC API.

    TODO: The STAC API has to be configured via a settings file.
    """
    if isinstance(collection_path, str):
        collection_path = Path(collection_path)

    uploader = Uploader.from_settings()
    uploader.upload_collection_and_items(collection_path)


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
