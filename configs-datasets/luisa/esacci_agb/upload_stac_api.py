from stacbuilder import *
from getpass import getpass

collection_path = "/home/vincent.verelst/vitotap/stac-catalog-builder/configs-datasets/luisa/esacci_agb/STAC/collection.json"

stac_api_pw = getpass("Enter password for stac api: ")

auth_settings = AuthSettings(
    enabled=True,
    interactive=False,
    token_url="https://sso.terrascope.be/auth/realms/terrascope/protocol/openid-connect/token",
    authorization_url= "https://sso.terrascope.be/auth/realms/terrascope/protocol/openid-connect/auth",
    client_id="terracatalogueclient",
    username = "vincent.verelst",
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
    collection_path=collection_path,
    settings=settings,
)