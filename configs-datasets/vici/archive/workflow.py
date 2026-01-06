import pprint
from getpass import getpass
from pathlib import Path
import os
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

current_dir = Path(__file__).parent.resolve()
# Collection configuration
catalog_version = "v1.0.0"
collection_config_path = current_dir / "config.json"
assert collection_config_path.exists(), f"Collection config file not found at {collection_config_path}"

# Input Paths
tiff_input_path = current_dir   
tiffs_glob = "*/*.tif"         
data_path = f"/data/open/vici/archive"
data_subpath = "/".join(data_path.split("/")[-2:])
# Output Paths
output_path = current_dir / "results" / catalog_version


# list input files
input_files = list_input_files(glob=tiffs_glob, input_dir=tiff_input_path, max_files=None)
print(f"Found {len(input_files)} input files. 10 first files:")
for i in input_files[:10]:
    print(i)



# list meta data
asset_metadata = list_asset_metadata(
    collection_config_path=collection_config_path, glob=tiffs_glob, input_dir=tiff_input_path, max_files=1
)
for k in asset_metadata:
    pprint.pprint(k.to_dict())


def postprocess_item(item: pystac.Item) -> pystac.Item:
    # Example postprocessing: add a custom property to the item

    for asset_name, asset_metadata in item.assets.items():
        # Extract filename from metadata.href instead of using asset_name directly
        full_filename = os.path.basename(asset_metadata.href)  # Extracts 'percentiles15_0101.tif'
        
        # Construct the correct local path
        # Add the alternate field to each asset
        # get country from path
        country = Path(asset_metadata.href).parent.name
        asset_metadata.href = f"https://services.terrascope.be/download/open/{data_subpath}/{country}/{full_filename}"
        
        asset_metadata.extra_fields["alternate"] = {
            "local": {
                "href": f"{data_path}/{country}/{full_filename}" 
            }
        }
        item.properties["country"] = country
    return item

# list items
stac_items, failed_files = list_stac_items(
    collection_config_path=collection_config_path,
    glob=tiffs_glob,
    input_dir=tiff_input_path,
    max_files=1000,
    item_postprocessor=postprocess_item,
)
print(f"Found {len(stac_items)} STAC items")
if failed_files:
    print(f"Failed files: {failed_files}")

if not stac_items:
    raise RuntimeError(
        "No STAC items created — check regex in config.json and folder structure"
    )

# build collection
build_collection(
    collection_config_path=collection_config_path,
    glob=tiffs_glob,
    input_dir=tiff_input_path,
    output_dir=output_path,
    item_postprocessor=postprocess_item,
    link_items=False,
)

collection_file = output_path.parent / "collection.json"


if collection_file.exists():
    validate_collection(collection_file=collection_file)
    print("Collection validation complete.")
else:
    raise FileNotFoundError(
        f"collection.json not found at {collection_file}. Build likely failed."
    )


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
    collection_path=collection_file,
    settings=settings,
)
print("Done uploading collection to STAC API")
