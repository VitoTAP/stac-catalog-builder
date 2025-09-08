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
tiff_input_path = Path("/data/open/luisa/continental_products/hanpp_harv_crop")
assert tiff_input_path.exists(), f"Path does not exist: {tiff_input_path}"
tiffs_glob = "*.tif"

# Output Paths
output_path = Path(__file__).parent.resolve() / "results"
test_output_path = output_path / "test" / catalog_version
publish_output_path = output_path / "publish" / catalog_version
overwrite = True

# list input files
# input_files = list_input_files(
#     glob=tiffs_glob,
#     input_dir=tiff_input_path,
#     max_files=10  # Remove this in final workflow!
# )
# print(f"Found {len(input_files)} input files. 5 first files:")
# for i in input_files[:5]: print(i) 

# list meta data
# asset_metadata = list_asset_metadata(
#     collection_config_path=collection_config_path,
#     glob=tiffs_glob,
#     input_dir=tiff_input_path,
#     max_files=1
# )
# for k in asset_metadata: 
#     pprint.pprint(k.to_dict())

def item_postprocessor(item: pystac.Item) -> pystac.Item:
    def replace_path_with_url(path: str) -> str:
        filename = path.split("/")[-1]
        return 'https://s3.waw3-1.cloudferro.com/swift/v1/luisa/hanpp_harv_crop/' + filename
    
    for asset in item.assets.values():
        asset.href = replace_path_with_url(asset.href)
        asset.extra_fields['raster:bands'][0]['nodata'] = 'nan'  # Ensure no data is set to 'nan' as a string

    return item


# list items
# stac_items, failed_files = list_stac_items(
#     collection_config_path=collection_config_path,
#     glob=tiffs_glob,
#     input_dir=tiff_input_path,
#     max_files=1,
#     item_postprocessor=item_postprocessor
# )
# print(f"Found {len(stac_items)} STAC items")
# if failed_files: print(f"Failed files: {failed_files}")

# print("First stac item:")
# pprint.pprint(stac_items[0].to_dict())



# build collection
build_collection(
    collection_config_path=collection_config_path,
    glob=tiffs_glob,
    input_dir=tiff_input_path,
    output_dir='./stac/',#test_output_path,
    overwrite=overwrite,
    link_items=True,
    item_postprocessor=item_postprocessor,
)

# validate collection
validate_collection(
    collection_file="./stac/collection.json",
)

