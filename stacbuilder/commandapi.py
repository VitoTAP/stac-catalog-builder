"""
This module provides the most important functions for building STAC collections and items.

The module is organized into the following functional areas:

**Collection Building:**
- build_collection: Build STAC collections from input files
- build_grouped_collections: Build multiple related STAC collections (deprecated)

**Data Exploration:**
- list_input_files: List files that match the configuration
- list_asset_metadata: Extract metadata from input files
- list_stac_items: Generate STAC items for testing

**Collection Management:**
- load_collection: Load existing STAC collections
- validate_collection: Validate STAC collection format

**STAC API Operations:**
- upload_to_stac_api: Upload collections and items to STAC API
- upload_items_to_stac_api: Upload only items to STAC API
"""

import logging
from pathlib import Path
from typing import Callable, List, Optional, Tuple

from deprecated import deprecated
from pystac import Collection, Item

from stacbuilder.builder import (
    AssetMetadataPipeline,
)
from stacbuilder.collector import FileCollector, MetadataCollector
from stacbuilder.config import CollectionConfig, FileCollectorConfig
from stacbuilder.metadata import AssetMetadata
from stacbuilder.stacapi import Settings, Uploader

log_level = logging.INFO
# create console handler with a higher log level
console_handler = logging.StreamHandler()
console_handler.setLevel(log_level)
# create formatter and add it to the handlers
formatter = logging.Formatter("%(levelname)-7s | %(asctime)s | %(message)s")
console_handler.setFormatter(formatter)
logging.basicConfig(handlers=[console_handler], level=log_level)
logging.getLogger("botocore").setLevel(logging.WARNING)
logging.getLogger("boto3").setLevel(logging.WARNING)

# Organized by functionality for better discoverability
__all__ = [
    # Collection building
    "build_collection",
    "build_grouped_collections",
    # Data exploration and listing
    "list_input_files",
    "list_asset_metadata",
    "list_stac_items",
    # Collection management
    "load_collection",
    "validate_collection",
    # STAC API operations
    "upload_to_stac_api",
    "upload_items_to_stac_api",
]


def build_collection(
    collection_config_path: Path,
    glob: str,
    input_dir: Path,
    output_dir: Path,
    max_files: Optional[int] = -1,
    link_items: bool = True,
    item_postprocessor: Optional[Callable] = None,
) -> None:
    """
    Build a STAC collection from a directory of files.

    :param collection_config_path: Path to the collection configuration file.
    :param glob: Glob pattern to match the files within the input_dir.
    :param input_dir: Root directory where the files are located.
    :param output_dir: Directory where the STAC collection will be saved.
    :param max_files: Maximum number of files to process.
    """
    collection_config_path = Path(collection_config_path).expanduser().absolute()
    coll_cfg = CollectionConfig.from_json_file(collection_config_path)
    file_coll_cfg = FileCollectorConfig(input_dir=input_dir, glob=glob, max_files=max_files)

    if output_dir and not isinstance(output_dir, Path):
        output_dir = Path(output_dir).expanduser().absolute()

    metadata_collector = MetadataCollector.from_config(
        collection_config=coll_cfg,
        file_coll_cfg=file_coll_cfg,
    )

    pipeline = AssetMetadataPipeline(
        collection_config=coll_cfg,
        metadata_collector=metadata_collector,
        output_dir=output_dir,
        link_items=link_items,
        item_postprocessor=item_postprocessor,
    )

    pipeline.build_collection()


@deprecated(reason="use build_collection instead")
def build_grouped_collections(
    collection_config_path: Path,
    glob: str,
    input_dir: Path,
    output_dir: Path,
    max_files: Optional[int] = -1,
) -> None:
    """
    Build a multiple STAC collections from a directory of files,
    where each collection groups related STAC items.

    The default grouping is per year.

    :param collection_config_path: Path to the collection configuration file.
    :param glob: Glob pattern to match the files within the input_dir.
    :param input_dir: Root directory where the files are located.
    :param output_dir: Directory where the STAC collection will be saved.
    :param max_files: Maximum number of files to process.
    """

    collection_config_path = Path(collection_config_path).expanduser().absolute()
    coll_cfg = CollectionConfig.from_json_file(collection_config_path)
    file_coll_cfg = FileCollectorConfig(input_dir=input_dir, glob=glob, max_files=max_files)

    if output_dir and not isinstance(output_dir, Path):
        output_dir = Path(output_dir).expanduser().absolute()

    metadata_collector = MetadataCollector.from_config(
        collection_config=coll_cfg,
        file_coll_cfg=file_coll_cfg,
    )

    pipeline = AssetMetadataPipeline(
        collection_config=coll_cfg,
        metadata_collector=metadata_collector,
        output_dir=output_dir,
    )

    pipeline.build_grouped_collections()


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
    if isinstance(input_dir, str):
        input_dir = Path(input_dir)
    collector = FileCollector(
        input_dir=input_dir,
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
) -> List[AssetMetadata]:
    """
    Return the AssetMetadata objects generated for each file.

    This is used to test the digestion of input files and check the configuration files.

    :param collection_config_path: Path to the collection configuration file.
    :param glob: Glob pattern to match the files within the input_dir.
    :param input_dir: Root directory where the files are located.
    :param max_files: Maximum number of files to process.
    :return: List of AssetMetadata objects for each file.
    """
    collection_config_path = Path(collection_config_path).expanduser().absolute()
    coll_cfg = CollectionConfig.from_json_file(collection_config_path)
    file_coll_cfg = FileCollectorConfig(input_dir=input_dir, glob=glob, max_files=max_files)
    collector = MetadataCollector.from_config(
        collection_config=coll_cfg,
        file_coll_cfg=file_coll_cfg,
    )
    collector.collect()

    return collector.metadata_list


def list_stac_items(
    collection_config_path: Path,
    glob: str,
    input_dir: Path,
    max_files: Optional[int] = -1,
    item_postprocessor: Optional[Callable] = None,
) -> Tuple[List[Item], List[Path]]:
    """
    Return the STAC items that are generated for each file and the files for which no stac item could be generated.

    This is used to test the creation of individual stac items from files.

    :param collection_config_path: Path to the collection configuration file.
    :param glob: Glob pattern to match the files within the input_dir.
    :param input_dir: Root directory where the files are located.
    :param max_files: Maximum number of files to process.
    :return: Tuple containing a List of STAC items and a list of files for which no item could be generated.
    """

    collection_config_path = Path(collection_config_path).expanduser().absolute()
    coll_cfg = CollectionConfig.from_json_file(collection_config_path)
    file_coll_cfg = FileCollectorConfig(input_dir=input_dir, glob=glob, max_files=max_files)
    metadata_collector = MetadataCollector.from_config(
        collection_config=coll_cfg,
        file_coll_cfg=file_coll_cfg,
    )
    pipeline = AssetMetadataPipeline(
        collection_config=coll_cfg,
        metadata_collector=metadata_collector,
        item_postprocessor=item_postprocessor,
    )

    stac_items = list(pipeline.collect_stac_items())
    files = list(metadata_collector.get_input_files())
    failed_files = [files[i] for i, item in enumerate(stac_items) if item is None]

    return stac_items, failed_files


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


def upload_to_stac_api(collection_path: Path, settings: Settings, limit: int = -1, offset: int = -1) -> None:
    """Upload a collection to the STAC API."""
    if not isinstance(collection_path, Path):
        collection_path = Path(collection_path)
    collection_path = collection_path.expanduser().absolute()

    uploader = Uploader.from_settings(settings)
    uploader.upload_collection_and_items(collection_path, items=collection_path.parent, limit=limit, offset=offset)


def upload_items_to_stac_api(collection_path: Path, settings: Settings, limit: int = -1, offset: int = -1) -> None:
    """Upload a collection to the STAC API."""
    if not isinstance(collection_path, Path):
        collection_path = Path(collection_path)
    collection_path = collection_path.expanduser().absolute()

    uploader = Uploader.from_settings(settings)
    uploader.upload_items(collection_path, items=collection_path.parent, limit=limit, offset=offset)
