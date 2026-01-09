import pprint
from pathlib import Path

import pystac

# run pip install -e . in the root directory to install this package
from stacbuilder import *

# Collection configuration
catalog_version = "v0.1"
collection_config_path = Path(__file__).parent.resolve() / "config-collection.json"

# Input Paths
terrascope_collection_id = "lcfm-tcd-10"
target = "CDSE"  # CDSE or LCFM or Terrascope
tiff_input_path = Path("/data/MTDA/LCFM/products/TCD-10/v100/tiles_latlon/3deg/")
assert tiff_input_path.exists(), f"Path does not exist: {tiff_input_path}"
tiffs_glob = "*/*/2020/*_MAP.tif"

# Output Paths
output_path = Path(__file__).parent.resolve() / "results"
test_output_path = output_path / target / catalog_version
publish_output_path = output_path / "publish" / catalog_version


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
    if target == "Terrascope":
        item.collection_id = terrascope_collection_id

        item.properties["auth:schemes"] = {
            "oidc": {
                "type": "openIdConnect",
                "description": "Authenticate with Terrascope OpenID Connect",
                "openIdConnectUrl": "https://sso.terrascope.be/auth/realms/terrascope/.well-known/openid-configuration",
            }
        }
        item.stac_extensions.append("https://stac-extensions.github.io/authentication/v1.1.0/schema.json")
        item.assets["MAP"].extra_fields["auth:refs"] = ["oidc"]

        item.assets["MAP"].extra_fields["alternate"] = {"local": {"href": "file://" + item.assets["MAP"].href}}
        item.stac_extensions.append("https://stac-extensions.github.io/alternate-assets/v1.2.0/schema.json")
        item.assets["MAP"].href = "https://services.terrascope.be/download/" + item.assets["MAP"].href[11:]
    elif target == "LCFM":
        s3_prepend = "s3://lcfm_waw3-1_4b82fdbbe2580bdfc4f595824922507c0d7cae2541c0799982/gaf/products/TCD-10/v003/tiles_latlon/3deg/"
        s3_prefix = "/".join(item.assets["MAP"].href.split("/")[-4:-1])
        s3_name = item.assets["MAP"].href.split("/")[-1].replace("_V12C_C232", "")
        item.assets["MAP"].href = s3_prepend + s3_prefix + "/" + s3_name
    elif target == "CDSE":
        s3_prepend = "s3://eodata/CLMS/landcover_landuse/dynamic_land_cover/tcd_pantropical_10m_yearly_v1/2020/01/01/"
        s3_folder = item.assets["MAP"].href.split("/")[-1].split(".")[0][:-4] + "_cog"
        s3_name = item.assets["MAP"].href.split("/")[-1]
        item.assets["MAP"].href = s3_prepend + s3_folder + "/" + s3_name
    else:
        raise ValueError(f"Unknown target: {target}")
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
    link_items=False,
    item_postprocessor=item_postprocessor,
)

# validate collection
validate_collection(
    collection_file=test_output_path / ".." / "collection.json",
)
