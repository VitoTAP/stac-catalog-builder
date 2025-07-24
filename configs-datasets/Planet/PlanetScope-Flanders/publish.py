from time import sleep
import pystac
from pathlib import Path
import pprint
from getpass import getpass
import logging

# run pip install -e . in the root directory to install this package
from stacbuilder import *

_logger = logging.getLogger(__name__)
_logger.setLevel(logging.DEBUG)

# Collection configuration
catalog_version = "v0.1"
collection_config_path = Path(__file__).parent.resolve() / "config-collection.json"

# Input Paths
# For Windows UNC path
tiff_input_path = Path("/vitodata/PlanetScope/prod/flanders/archive/")
assert tiff_input_path.exists(), f"Path does not exist: {tiff_input_path}"
tiffs_glob = "*/*/*/*/*/*_clip.tif"

# Output Paths
output_path = Path(__file__).parent.resolve() / "results"
test_output_path = output_path / "test" / catalog_version
publish_output_path = output_path / "publish" / catalog_version
overwrite = True

stac_api_pw = getpass("Enter password for stac api: ")

auth_settings = AuthSettings(
    enabled=True,
    interactive=False,
    token_url="https://sso.terrascope.be/auth/realms/terrascope/protocol/openid-connect/token",
    authorization_url= "https://sso.terrascope.be/auth/realms/terrascope/protocol/openid-connect/auth",
    client_id="terracatalogueclient",
    username = "jeroenwannijn",
    password = stac_api_pw,
)
settings = Settings(
    auth=auth_settings,
    stac_api_url="https://stac.openeo.vito.be/",
    collection_auth_info={
            "_auth": {
                "read": ["anonymous"],
                "write": ["stac-openeo-admin", "stac-openeo-editor"]
            }
        },
    bulk_size=1000,  
)
upload_to_stac_api(
    collection_path = publish_output_path / "collection.json",
    settings=settings,
)
print("Sleeping for 60 seconds to allow the STAC API to update")
sleep(60)

