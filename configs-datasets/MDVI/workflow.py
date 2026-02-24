from getpass import getpass
from pathlib import Path

import pystac

# Needs to be installed to load .env files
from dotenv import load_dotenv
from upath import UPath

from stacbuilder import (
    upload_to_stac_api,
    validate_collection,
)
from stacbuilder.commandapi import build_collection
from stacbuilder.stacapi.config import AuthSettings, Settings

current_file_path = Path(__file__).parent


# S3 credentials setup
# Load environment variables from .env file
env_path = current_file_path / ".env"
load_dotenv(env_path)

# Collection configuration
catalog_version = "v0.1"
collection_config_path = current_file_path / "config-collection.json"

# Input Paths
tiff_input_path = UPath("s3://hr-vpp-auxdata/MDVI/101/")
assert tiff_input_path.exists(), f"Input path {tiff_input_path} does not exist."
tiffs_glob = "*.tif"
# Output Paths
output_path = current_file_path / "results"


def postprocess_item(item: pystac.Item) -> pystac.Item:
    # Example postprocessing: add a custom property to the item
    item.properties["version"] = "101"
    return item


# # build collection
build_collection(
    collection_config_path=collection_config_path,
    glob=tiffs_glob,
    input_dir=tiff_input_path,
    output_dir=output_path,
    item_postprocessor=postprocess_item,
    single_asset_per_item=True,
    link_items=False,
)

# validate collection
validate_collection(
    collection_file=output_path / "collection.json",
)

print("Collection validation complete.")

# Optional: Upload to STAC API
# The openeo STAC API is used for this example, but you can use any STAC API that supports the STAC API specification.
# The STAC API must be configured to accept the collection and items you are uploading.

auth_settings = AuthSettings(
    enabled=True,
    interactive=False,
    token_url="https://sso.terrascope.be/auth/realms/terrascope/protocol/openid-connect/token",
    authorization_url="https://sso.terrascope.be/auth/realms/terrascope/protocol/openid-connect/auth",
    client_id="terracatalogueclient",
    username=input("Enter username for STAC API: "),
    password=getpass("Enter password for STAC API: "),
)
settings = Settings(
    auth=auth_settings,
    stac_api_url="https://stac.openeo.vito.be/",
    collection_auth_info={"_auth": {"read": ["anonymous"], "write": ["stac-openeo-admin", "stac-openeo-editor"]}},
    bulk_size=1000,
)
upload_to_stac_api(
    collection_path=output_path / "collection.json",
    settings=settings,
)
print("Done uploading collection to STAC API")
