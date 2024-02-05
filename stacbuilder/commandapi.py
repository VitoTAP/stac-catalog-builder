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
import pprint

from pystac import Collection
from typing import Dict, Hashable, List, Optional


from stacbuilder.builder import (
    AssetMetadataPipeline,
    FileCollector,
    GeoTiffPipeline,
    GeodataframeExporter,
    PostProcessSTACCollectionFile,
)
from stacbuilder.config import CollectionConfig, FileCollectorConfig
from stacbuilder.metadata import AssetMetadata
from stacbuilder.terracatalog import HRLVPPMetadataCollector


def build_collection(
    collection_config_path: Path,
    glob: str,
    input_dir: Path,
    output_dir: Path,
    overwrite: bool,
    max_files: Optional[int] = -1,
    save_dataframe: Optional[bool] = False,
)-> None:
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
)-> None:
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
)-> list[Path]:
    """
    Searches the files that are found with the current configuration.

    This can be used to test the glob pattern and the input directory.
    
    :param glob: Glob pattern to match the files within the input_dir.
    :param input_dir: Root directory where the files are located.
    :param max_files: Maximum number of files to process.
    :return: List containing paths of all the found files.
    """

    collector = FileCollector()
    collector.input_dir = Path(input_dir)
    collector.glob = glob
    collector.max_files = max_files
    collector.collect()
    return collector.input_files


def list_asset_metadata(
    collection_config_path: Path,
    glob: str,
    input_dir: Path,
    max_files: Optional[int] = -1,
    save_dataframe: bool = False,
)-> Dict[Hashable, List[AssetMetadata]]:
    """
    Return the AssetMetadata objects generated for each file.

    This is used to test the digestion of input files and check the configuration files.

    :param collection_config_path: Path to the collection configuration file.
    :param glob: Glob pattern to match the files within the input_dir.
    :param input_dir: Root directory where the files are located.
    :param max_files: Maximum number of files to process.
    :param save_dataframe: Save the geodataframe of the metadata.
    :return: Dictionary containing the groups as keys and the AssetMetadata objects for each file. If the collection is not grouped, the key is an empty string.
    """

    collection_config_path = Path(collection_config_path).expanduser().absolute()
    coll_cfg = CollectionConfig.from_json_file(collection_config_path)
    file_coll_cfg = FileCollectorConfig(input_dir=input_dir, glob=glob, max_files=max_files)
    pipeline = GeoTiffPipeline.from_config(collection_config=coll_cfg, file_coll_cfg=file_coll_cfg)

    if save_dataframe:
        df = pipeline.get_metadata_as_geodataframe()
        # TODO: Want better directory to save geodata, maybe use save_dataframe as path instead of flag.
        out_dir = Path("tmp") / coll_cfg.collection_id / "visualization_list-assetmetadata"
        GeodataframeExporter.save_geodataframe(df, out_dir, "metadata_table")

    if not pipeline.has_grouping:
        return {"": list(pipeline.get_metadata())}
    else:
        return pipeline.get_metadata_groups()


def list_stac_items(
    collection_config_path: Path,
    glob: str,
    input_dir: Path,
    max_files: Optional[int] = -1,
    save_dataframe: bool = False,
)-> (List[Collection], List[Path]):
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

    stac_items = list(pipeline.collect_stac_items())
    files = list(pipeline.get_input_files())
    failed_files = [files[i] for i, item in enumerate(stac_items) if item is None]

    return stac_items, failed_files
    

def postprocess_collection(
    collection_file: Path,
    collection_config_path: Path,
    output_dir: Optional[Path] = None,
)-> None:
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
)-> Collection:
    """
    Load and return the STAC collection in 'collection_file'.
    
    :param collection_file: Path to the STAC collection file.
    :return: The STAC collection object.
    """
    return Collection.from_file(collection_file)


def validate_collection(
    collection_file: Path,
)-> None:
    """
    Validate a STAC collection.
    
    :param collection_file: Path to the STAC collection file.
    """
    collection = Collection.from_file(collection_file)
    collection.validate_all()


def vpp_list_metadata(
    max_products: Optional[int] = -1,
):
    """Show the AssetMetadata objects that are generated for each VPP product.

    This is used to test the conversion and check the configuration files.
    """
    collector = HRLVPPMetadataCollector()
    COLLECTION_ID = "copernicus_r_3035_x_m_hrvpp-st_p_2017-now_v01"
    collector.collection_id = COLLECTION_ID
    collector.max_products = max_products
    collector.collect()

    for md in collector.metadata_list:
        pprint.pprint(md.to_dict())


def vpp_list_stac_items(
    collection_config_path: Path,
    max_products: Optional[int] = -1,
):
    """Show the STAC items that are generated for each VPP product.

    This is used to test the conversion and check the configuration files.
    """

    # In the end the HRLVPPMetadataCollector would not really need configuration
    # We do need to process all collections, but perhaps we we want to
    # keep the collection ID as a parameter so we can run it selectively.
    # We would just need a list of all collection IDs we want to process.
    collector = HRLVPPMetadataCollector()
    COLLECTION_ID = "copernicus_r_3035_x_m_hrvpp-st_p_2017-now_v01"
    collector.collection_id = COLLECTION_ID
    collector.max_products = max_products
    collector.collect()

    collection_config_path = Path(collection_config_path).expanduser().absolute()
    coll_cfg = CollectionConfig.from_json_file(collection_config_path)
    pipeline = AssetMetadataPipeline.from_config(
        metadata_collector=collector,
        collection_config=coll_cfg,
        output_dir=None,
        overwrite=False,
    )

    for item in list(pipeline.collect_stac_items()):
        pprint.pprint(item.to_dict())


def vpp_build_collection(
    collection_config_path: Path,
    output_dir: Path,
    overwrite: bool,
    max_products: Optional[int] = -1,
    # save_dataframe: Optional[bool] = False,
):
    """Build a STAC collection for one of the collections in HRL VPP (OpenSearch)."""

    collector = HRLVPPMetadataCollector()
    COLLECTION_ID = "copernicus_r_3035_x_m_hrvpp-st_p_2017-now_v01"
    collector.collection_id = COLLECTION_ID
    collector.max_products = max_products
    collector.collect()

    collection_config_path = Path(collection_config_path).expanduser().absolute()
    coll_cfg = CollectionConfig.from_json_file(collection_config_path)
    pipeline = AssetMetadataPipeline.from_config(
        metadata_collector=collector,
        collection_config=coll_cfg,
        output_dir=output_dir,
        overwrite=overwrite,
    )

    pipeline.from_config()
    pipeline.build_collection()

    # if save_dataframe:
    #     GeodataframeExporter.export_item_bboxes(pipeline.collection)
