# import pprint
from pathlib import Path

import pystac

# run pip install -e . in the root directory to install this package
from stacbuilder import (  # list_asset_metadata,; list_input_files,; list_stac_items,
    build_collection,
    validate_collection,
)

# Collection configuration
catalog_version = "v0.1"
collection_config_path = Path(__file__).parent.resolve() / "config-collection.json"

# Input Paths
tiff_input_path = Path("/vitodata/vegteam_los/")
assert tiff_input_path.exists(), f"Path does not exist: {tiff_input_path}"
tiffs_glob = "*/products/LCFM/LOS/v100/tiles_utm/*/*/*/*/*/*_PROBS_60M.tif"

# Output Paths
output_path = Path(__file__).parent.resolve() / "results"
test_output_path = output_path / "test" / catalog_version
publish_output_path = output_path / "publish" / catalog_version


# # list input files
# input_files = list_input_files(glob=tiffs_glob, input_dir=tiff_input_path, max_files=10)
# print(f"Found {len(input_files)} input files. 5 first files:")
# for i in input_files[:5]:
#     print(i)

# # list meta data
# asset_metadata = list_asset_metadata(
#     collection_config_path=collection_config_path, glob=tiffs_glob, input_dir=tiff_input_path, max_files=1
# )
# for k in asset_metadata:
#     pprint.pprint(k.to_dict())

# exit()


def item_postprocessor(item: pystac.Item) -> pystac.Item:
    tileId = item.id.split("_")[-3]
    item.properties["tileId"] = tileId

    item.properties["product_version"] = "v100"
    return item


# list items
# stac_items, failed_files = list_stac_items(
#     collection_config_path=collection_config_path,
#     glob=tiffs_glob,
#     input_dir=tiff_input_path,
#     max_files=1,
#     item_postprocessor=item_postprocessor,
# )
# print(f"Found {len(stac_items)} STAC items")
# if failed_files:
#     print(f"Failed files: {failed_files}")

# print("First stac item:")
# pprint.pprint(stac_items[0].to_dict())
# exit()
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
    collection_file=test_output_path / "collection.json",
)
