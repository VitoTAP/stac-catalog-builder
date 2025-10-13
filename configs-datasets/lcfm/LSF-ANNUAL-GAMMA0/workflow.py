<<<<<<< HEAD

from time import sleep
import pystac
from upath import UPath
from pathlib import Path
import pprint
from getpass import getpass

# run pip install -e . in the root directory to install this package
from stacbuilder import *

=======
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
>>>>>>> origin/main

# Collection configuration
catalog_version = "v0.1"
collection_config_path = Path(__file__).parent.resolve() / "config-collection.json"

# Input Paths
tiff_input_path = Path("/vitodata/vegteam_vol3/products/LCFM/LSF-ANNUAL-GAMMA0/v001/tiles_utm")
assert tiff_input_path.exists(), f"Path does not exist: {tiff_input_path}"
tiffs_glob = "*/*/*/2020/*.tif"

# Output Paths
output_path = Path(__file__).parent.resolve() / "results"
test_output_path = output_path / "test" / catalog_version
publish_output_path = output_path / "publish" / catalog_version
<<<<<<< HEAD
overwrite = True


# list input files
input_files = list_input_files(
    glob=tiffs_glob,
    input_dir=tiff_input_path,
    max_files=10
)
print(f"Found {len(input_files)} input files. 5 first files:")
for i in input_files[:5]: print(i) 

=======


# list input files
input_files = list_input_files(glob=tiffs_glob, input_dir=tiff_input_path, max_files=10)
print(f"Found {len(input_files)} input files. 5 first files:")
for i in input_files[:5]:
    print(i)
>>>>>>> origin/main


# list meta data
asset_metadata = list_asset_metadata(
<<<<<<< HEAD
    collection_config_path=collection_config_path,
    glob=tiffs_glob,
    input_dir=tiff_input_path,
    max_files=1
)
for k in asset_metadata: 
    pprint.pprint(k.to_dict())

=======
    collection_config_path=collection_config_path, glob=tiffs_glob, input_dir=tiff_input_path, max_files=1
)
for k in asset_metadata:
    pprint.pprint(k.to_dict())


>>>>>>> origin/main
def item_postprocessor(item: pystac.Item) -> pystac.Item:
    item.properties["proj:code"] = "EPSG:" + str(item.properties["proj:epsg"])
    del item.properties["proj:epsg"]
    item.stac_extensions[2] = "https://stac-extensions.github.io/projection/v2.0.0/schema.json"
    item.properties["tileId"] = item.properties["product_tile"]
    del item.properties["product_tile"]
<<<<<<< HEAD
    #item.assets["map"].href = "s3://lcfm_waw3-1_4b82fdbbe2580bdfc4f595824922507c0d7cae2541c0799982/vito/products/LCM-10/v001/latlon_3deg_tiles/2020/MAP/" + item.assets["map"].href.split("/")[-1]    
    return item

=======
    # item.assets["map"].href = "s3://lcfm_waw3-1_4b82fdbbe2580bdfc4f595824922507c0d7cae2541c0799982/vito/products/LCM-10/v001/latlon_3deg_tiles/2020/MAP/" + item.assets["map"].href.split("/")[-1]
    return item


>>>>>>> origin/main
# list items
stac_items, failed_files = list_stac_items(
    collection_config_path=collection_config_path,
    glob=tiffs_glob,
    input_dir=tiff_input_path,
    max_files=1,
<<<<<<< HEAD
    item_postprocessor=item_postprocessor
)
print(f"Found {len(stac_items)} STAC items")
if failed_files: print(f"Failed files: {failed_files}")
=======
    item_postprocessor=item_postprocessor,
)
print(f"Found {len(stac_items)} STAC items")
if failed_files:
    print(f"Failed files: {failed_files}")
>>>>>>> origin/main

print("First stac item:")
pprint.pprint(stac_items[0].to_dict())

# build collection
build_collection(
    collection_config_path=collection_config_path,
    glob=tiffs_glob,
    input_dir=tiff_input_path,
    output_dir=test_output_path,
<<<<<<< HEAD
    overwrite=overwrite,
=======
>>>>>>> origin/main
    link_items=False,
    item_postprocessor=item_postprocessor,
)

# validate collection
validate_collection(
    collection_file=test_output_path / "collection.json",
)
