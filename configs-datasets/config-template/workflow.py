import pprint
from getpass import getpass
from pathlib import Path

import pystac

from stacbuilder import (
    build_collection,
    list_asset_metadata,
    list_input_files,
    list_stac_items,
    upload_to_stac_api,
    validate_collection,
)
from stacbuilder.stacapi.config import AuthSettings, Settings

# Collection configuration
catalog_version = "v0.1"
collection_config_path = Path("config-collection.json")

# Input Paths
tiff_input_path = Path("/path/to/tiffs")
tiffs_glob = "*/*.tif"

# Output Paths
output_path = Path("results") / catalog_version


# list input files
input_files = list_input_files(glob=tiffs_glob, input_dir=tiff_input_path, max_files=None)
print(f"Found {len(input_files)} input files. 5 first files:")
for i in input_files[:5]:
    print(i)


# list meta data
asset_metadata = list_asset_metadata(
    collection_config_path=collection_config_path, glob=tiffs_glob, input_dir=tiff_input_path, max_files=1
)
for k in asset_metadata:
    pprint.pprint(k.to_dict())


def postprocess_item(item: pystac.Item) -> pystac.Item:
    # Example postprocessing: add a custom property to the item
    item.properties["custom_property"] = "custom_value"
    return item


# list items
stac_items, failed_files = list_stac_items(
    collection_config_path=collection_config_path,
    glob=tiffs_glob,
    input_dir=tiff_input_path,
    max_files=10,
    item_postprocessor=postprocess_item,
)
print(f"Found {len(stac_items)} STAC items")
if failed_files:
    print(f"Failed files: {failed_files}")


print("First stac item:")
stac_items[0]


# build collection
build_collection(
    collection_config_path=collection_config_path,
    glob=tiffs_glob,
    input_dir=tiff_input_path,
    output_dir=output_path,
    item_postprocessor=postprocess_item,
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
