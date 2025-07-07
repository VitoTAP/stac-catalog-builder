import pprint
from pathlib import Path

import pystac

# run pip install -e . in the root directory to install this package
from stacbuilder import *

# Collection configuration
catalog_version = "v0.1"
collection_config_path = Path(__file__).parent.resolve() / "config-collection.json"

# Input Paths
tiff_input_path = Path("/data/MTDA/LCFM/products/LCM-10/v100/tiles_latlon/3deg/")
assert tiff_input_path.exists(), f"Path does not exist: {tiff_input_path}"
tiffs_glob = "*/*/2020/*_MAP.tif"

# Output Paths
output_path = Path(__file__).parent.resolve() / "results"
test_output_path = output_path / "test" / catalog_version
publish_output_path = output_path / "publish" / catalog_version
overwrite = True


# list input files
input_files = list_input_files(glob=tiffs_glob, input_dir=tiff_input_path, max_files=10)
print(f"Found {len(input_files)} input files. 5 first files:")
for i in input_files[:5]:
    print(i)


# list meta data
asset_metadata = list_asset_metadata(
    collection_config_path=collection_config_path, glob=tiffs_glob, input_dir=tiff_input_path, max_files=1
)
for k in asset_metadata:
    pprint.pprint(k.to_dict())


def item_postprocessor(item: pystac.Item) -> pystac.Item:
    # item.properties["proj:code"] = "EPSG:" + str(item.properties["proj:epsg"])
    # del item.properties["proj:epsg"]
    # item.stac_extensions[2] = "https://stac-extensions.github.io/projection/v2.0.0/schema.json"
    # item.id = item.id.replace("_MAP_V12C_C232", "_MAP")

    # item.properties["tileId"] = item.properties["product_tile"]
    # del item.properties["product_tile"]

    item.collection_id = "lcfm-lcm-10"

    item.properties["auth:schemes"] = {
        "oidc": {
            "type": "openIdConnect",
            "description": "Authenticate with Terrascope OpenID Connect",
            "openIdConnectUrl": "https://sso.terrascope.be/auth/realms/terrascope/.well-known/openid-configuration",
        }
    }
    item.stac_extensions[2] = "https://stac-extensions.github.io/authentication/v1.1.0/schema.json"

    item.assets["MAP"].extra_fields["auth:refs"] = ["oidc"]

    item.assets["MAP"].extra_fields["alternate"] = {"local": {"href": "file://" + item.assets["MAP"].href}}
    item.stac_extensions[4] = "https://stac-extensions.github.io/alternate-assets/v1.2.0/schema.json"
    item.assets["MAP"].href = "https://services.terrascope.be/download/" + item.assets["MAP"].href[11:]

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
