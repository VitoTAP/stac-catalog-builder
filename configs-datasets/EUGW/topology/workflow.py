# %%
# from time import sleep
from time import sleep
import pystac
from pathlib import Path
from upath import UPath
import pprint
from getpass import getpass
import logging
import os
import configparser

# run pip install -e . in the root directory to install this package
from stacbuilder import (
    CollectionConfig,
    list_input_files,
    list_asset_metadata,
    list_stac_items,
    build_collection,
    validate_collection,
    upload_to_stac_api,
)
from stacbuilder.stacapi.config import AuthSettings, Settings

_logger = logging.getLogger(__name__)
_logger.setLevel(logging.INFO)

# Collection configuration
catalog_version = "v0.1"
collection_config_path = Path(__file__).parent.resolve() / "config-collection.json"
collection_config = CollectionConfig.from_json_file(collection_config_path)
collection_name = collection_config.collection_id

# Input Paths
# Set environment variables for AWS S3
config = configparser.ConfigParser()
config.read(Path(__file__).parent / "eugw_gisat.conf")
os.environ["AWS_ACCESS_KEY_ID"] = config["EUGrasslandwatch"]["access_key_id"]
os.environ["AWS_SECRET_ACCESS_KEY"] = config["EUGrasslandwatch"]["secret_access_key"]
os.environ["AWS_ENDPOINT_URL_S3"] = config["EUGrasslandwatch"]["endpoint_url"]
os.environ["AWS_S3_ENDPOINT"] = config["EUGrasslandwatch"]["endpoint"]
os.environ["AWS_VIRTUAL_HOSTING"] = "FALSE"
os.environ["AWS_DEFAULT_REGION"] = "default"
os.environ["CPL_VSIL_CURL_CHUNK_SIZE"] = "10485760"
tiff_input_path = UPath(f"s3://topography/{ collection_name }")
assert tiff_input_path.exists(), f"Path does not exist: {tiff_input_path}"

# Output Paths
output_path = Path(__file__).parent.resolve() / "results"
stac_output_path = output_path / collection_name / catalog_version
overwrite = True

tiffs_glob = "*.tif"


# list input files
input_files = list_input_files(glob=tiffs_glob, input_dir=tiff_input_path, max_files=5)
print(f"Found {len(input_files)} input files. 5 first files:")
for i in input_files[:5]:
    print(i)


# %%
# list meta data
asset_metadata = list_asset_metadata(
    collection_config_path=collection_config_path, glob=tiffs_glob, input_dir=tiff_input_path, max_files=1
)
for k in asset_metadata:
    pprint.pprint(k.to_dict())


def item_postprocessor(item: pystac.Item) -> pystac.Item:
    item.properties["tileId"] = item.properties["product_tile"]
    del item.properties["product_tile"]
    return item


# list items
stac_items, failed_files = list_stac_items(
    collection_config_path=collection_config_path,
    glob=tiffs_glob,
    input_dir=tiff_input_path,
    max_files=1,
    item_postprocessor=item_postprocessor,
)
print(f"Found {len(stac_items)} STAC items")
if failed_files:
    print(f"Failed files: {failed_files}")

print("First stac item:")
pprint.pprint(stac_items[0].to_dict())


stac_api_pw = getpass("Enter password for stac api: ")
# build collection
build_collection(
    collection_config_path=collection_config_path,
    glob=tiffs_glob,
    input_dir=tiff_input_path,
    output_dir=stac_output_path,
    overwrite=overwrite,
    link_items=False,
    item_postprocessor=item_postprocessor,
)

# validate collection
validate_collection(
    collection_file=stac_output_path / "collection.json",
)


auth_settings = AuthSettings(
    enabled=True,
    interactive=False,
    token_url="https://sso.terrascope.be/auth/realms/terrascope/protocol/openid-connect/token",
    authorization_url="https://sso.terrascope.be/auth/realms/terrascope/protocol/openid-connect/auth",
    client_id="terracatalogueclient",
    username="victor.verhaert",
    password=stac_api_pw,
)
settings = Settings(
    auth=auth_settings,
    stac_api_url="https://stac.openeo.vito.be/",
    collection_auth_info={"_auth": {"read": ["anonymous"], "write": ["stac-openeo-admin", "stac-openeo-editor"]}},
    bulk_size=1000,
)
upload_to_stac_api(
    collection_path=stac_output_path / "collection.json",
    settings=settings,
)
_logger.info("Sleeping for 60 seconds to allow the STAC API to update")
sleep(60)
_logger.info("Done")
