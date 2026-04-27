import os
import sys
from pathlib import Path

from dotenv import load_dotenv
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
    "load_env",
]


def load_env(env_file: str | Path = ".env", *, override: bool = True) -> Path:
    env_path = Path(env_file)
    if not env_path.exists():
        raise FileNotFoundError(f".env file not found: {env_path}")

    load_dotenv(dotenv_path=env_path, override=override)

    required_s3_env_vars = [
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
        "AWS_ENDPOINT_URL_S3",
        "AWS_S3_ENDPOINT",
    ]
    missing_s3_env_vars = [env_var for env_var in required_s3_env_vars if not os.getenv(env_var)]
    if missing_s3_env_vars:
        missing_vars_str = ", ".join(missing_s3_env_vars)
        raise ValueError(f"Missing required .env variable(s): {missing_vars_str}")

    os.environ.setdefault("AWS_VIRTUAL_HOSTING", "FALSE")
    os.environ.setdefault("AWS_DEFAULT_REGION", "default")
    os.environ.setdefault("CPL_VSIL_CURL_CHUNK_SIZE", "10485760")

    return env_path


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
