from getpass import getpass
from pathlib import Path

from loguru import logger

from stacbuilder import (
    upload_to_stac_api,
)
from stacbuilder.stacapi.config import AuthSettings, Settings

CWD = Path(__file__).parent

logger.add(
    CWD / "upload.log",
    level="DEBUG",
)
catalog_version = "v01"
output_path = CWD / "results" / catalog_version

username = input("Enter username for STAC API: ")
password = getpass("Enter password for STAC API: ")


auth_settings = AuthSettings(
    enabled=True,
    interactive=False,
    token_url="https://sso.terrascope.be/auth/realms/terrascope/protocol/openid-connect/token",
    authorization_url="https://sso.terrascope.be/auth/realms/terrascope/protocol/openid-connect/auth",
    client_id="terracatalogueclient",
    username=username,
    password=password,
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
