import sys

from loguru import logger

from stacbuilder._version import __version__
from stacbuilder.builder import AssetMetadataPipeline
from stacbuilder.commandapi import (
    build_collection,
    build_grouped_collections,
    list_asset_metadata,
    list_input_files,
    list_stac_items,
    load_collection,
    upload_items_to_stac_api,
    upload_to_stac_api,
    validate_collection,
)
from stacbuilder.config import (
    CollectionConfig,
    FileCollectorConfig,
)
from stacbuilder.metadata import AssetMetadata
from stacbuilder.stacapi import AuthSettings, Settings

__all__ = [
    "__version__",
    "build_collection",
    "build_grouped_collections",
    "list_input_files",
    "list_asset_metadata",
    "list_stac_items",
    "load_collection",
    "validate_collection",
    "upload_to_stac_api",
    "upload_items_to_stac_api",
    "AuthSettings",
    "Settings",
    "CollectionConfig",
    "FileCollectorConfig",
    "AssetMetadata",
    "AssetMetadataPipeline",
]


# Configure default logging on module import
# Remove any existing handlers
logger.remove()

# Add console handler with sensible defaults
logger.add(
    sys.stderr,
    format=("<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <level>{message}</level>"),
    level="INFO",
    colorize=True,
)

# Suppress verbose logging from third-party libraries
logger.disable("botocore")
logger.disable("boto3")
logger.disable("urllib3")
