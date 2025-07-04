import pprint
from pathlib import Path

import pystac

# run pip install -e . in the root directory to install this package
from stacbuilder import (
    build_collection,
    list_asset_metadata,
    list_input_files,
    list_stac_items,
    validate_collection,
)

# Collection configuration
product = "LCM-100"
version = "v005"
postfix = "_V12C_C232"
level = "30deg" if product == "LCM-100" else "3deg"
catalog_version = "v0.1"
collection_config_path = Path(__file__).parent.resolve() / "config-collection.json"

# Input Paths
tiff_input_path = Path(f"/vitodata/vegteam_vol2/products/LCFM/{product}/{version}/tiles_latlon/{level}/")
print(tiff_input_path)
assert tiff_input_path.exists(), f"Path does not exist: {tiff_input_path}"
tiffs_glob = "*/*/2020/*.tif"

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
    # Proj V2
    # item.properties["proj:code"] = "EPSG:" + str(item.properties["proj:epsg"])
    # del item.properties["proj:epsg"]
    # item.stac_extensions[2] = "https://stac-extensions.github.io/projection/v2.0.0/schema.json"
    item.id = item.id.replace(postfix, "")

    item.properties["tileId"] = item.properties["product_tile"]
    del item.properties["product_tile"]

    item.properties["product_version"] = f"{version}"

    # s3_prepend = f"s3://lcfm_waw3-1_4b82fdbbe2580bdfc4f595824922507c0d7cae2541c0799982/vito/products/{product}/{version}/tiles_latlon/{level}/"
    # s3_prefix = "/".join(item.assets["MAP"].href.split("/")[-4:-1])
    # s3_name = item.assets["MAP"].href.split("/")[-1].replace(postfix, "")
    # item.assets["MAP"].href = s3_prepend + s3_prefix + "/" + s3_name
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
