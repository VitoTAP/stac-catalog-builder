# from time import sleep
import configparser
import logging
import os
from getpass import getpass
from pathlib import Path

from upath import UPath

# run pip install -e . in the root directory to install this package
from stacbuilder import (
    CollectionConfig,
    build_collection,
    upload_to_stac_api,
    validate_collection,
)
from stacbuilder.stacapi.config import AuthSettings, Settings

_logger = logging.getLogger(__name__)
_logger.setLevel(logging.INFO)

# Collection configuration
collection_config_path = Path(__file__).parent.resolve() / "config-collection-WET30_LAEA.json"
collection_config = CollectionConfig.from_json_file(collection_config_path)
collection_name = collection_config.collection_id
_logger.info(f"Collection name: {collection_name}")

# Input Paths
# Set environment variables for AWS S3
config = configparser.ConfigParser()
if not (Path(__file__).parent / "eugw_gisat.conf").exists():
    raise FileNotFoundError("Configuration file for S3 not found")
config.read(Path(__file__).parent / "eugw_gisat.conf")
os.environ["AWS_ACCESS_KEY_ID"] = config["EUGrasslandwatch"]["access_key_id"]
os.environ["AWS_SECRET_ACCESS_KEY"] = config["EUGrasslandwatch"]["secret_access_key"]
os.environ["AWS_ENDPOINT_URL_S3"] = config["EUGrasslandwatch"]["endpoint_url"]
os.environ["AWS_S3_ENDPOINT"] = config["EUGrasslandwatch"]["endpoint"]
os.environ["AWS_VIRTUAL_HOSTING"] = "FALSE"
os.environ["AWS_DEFAULT_REGION"] = "default"
os.environ["CPL_VSIL_CURL_CHUNK_SIZE"] = "10485760"
tiff_input_path = UPath(f"s3://topography/{collection_name}")
assert tiff_input_path.exists(), f"Path does not exist: {tiff_input_path}"

# Output Paths
output_path = Path(__file__).parent.resolve() / "results"
stac_output_path = output_path / collection_name

tiffs_glob = "*.tif"  # CLCBB*/ WAW*/

# print(list_input_files(input_dir=tiff_input_path, glob=tiffs_glob))
# exit()
stac_api_un = input("Enter username for STAC API: ")
stac_api_pw = getpass("Enter password for stac api: ")

# build collection
build_collection(
    collection_config_path=collection_config_path,
    glob=tiffs_glob,
    input_dir=tiff_input_path,
    output_dir=stac_output_path,
    link_items=False,
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
    username=stac_api_un,
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
_logger.info(f"Done uploading {collection_name} to STAC API")
