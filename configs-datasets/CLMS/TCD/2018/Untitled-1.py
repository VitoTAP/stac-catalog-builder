# %%
from upath import UPath
from pathlib import Path
import pprint
from getpass import getpass

# run pip install -e . in the root directory to install this package
from stacbuilder import (
    build_collection,
    validate_collection,
    upload_to_stac_api,
    list_input_files,
    list_asset_metadata,
    list_stac_items,
)
from stacbuilder.stacapi.config import AuthSettings, Settings

# %%
# Collection configuration
catalog_version = "v0.2"
collection_config_path = Path(__file__).parent / "config-collection.json"

# Input Paths
tiff_input_path = UPath(
    "s3://eodata/CLMS/Pan-European/High_Resolution_Layers/Forests/Tree_Cover_Density/Status_Maps/Tree_Cover_Density_2018/"
)
tiffs_glob = "*/*/*_03035_v020.tif"

# Output Paths
output_path = Path("results")
test_output_path = output_path / "test" / catalog_version
publish_output_path = output_path / "publish" / catalog_version
overwrite = True

# %%
# list input files
input_files = list_input_files(glob=tiffs_glob, input_dir=tiff_input_path, max_files=None)
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
exit()


# %%
def item_postprocessor(item):
    item.properties["proj:epsg"] = 3035
    return item


# %%
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

# %%
print("First stac item:")
stac_items[0]

# %%
# build collection
build_collection(
    collection_config_path=collection_config_path,
    glob=tiffs_glob,
    input_dir=tiff_input_path,
    output_dir=test_output_path,
    overwrite=overwrite,
    link_items=False,
    item_postprocessor=item_postprocessor,
)

# validate collection
validate_collection(
    collection_file=test_output_path / "collection.json",
)

# %%
auth_settings = AuthSettings(
    enabled=True,
    interactive=False,
    token_url="https://sso.terrascope.be/auth/realms/terrascope/protocol/openid-connect/token",
    authorization_url="https://sso.terrascope.be/auth/realms/terrascope/protocol/openid-connect/auth",
    client_id="terracatalogueclient",
    username="victor.verhaert",
    password=getpass(),
)
settings = Settings(
    auth=auth_settings,
    stac_api_url="https://stac.openeo.vito.be/",
    collection_auth_info={"_auth": {"read": ["anonymous"], "write": ["stac-openeo-admin", "stac-openeo-editor"]}},
    bulk_size=1000,
)
upload_to_stac_api(
    collection_path=test_output_path / "collection.json",
    settings=settings,
)
