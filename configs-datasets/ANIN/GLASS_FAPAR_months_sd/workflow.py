import pprint
from pathlib import Path
from shutil import rmtree

# run pip install -e . in the root directory to install this package
from stacbuilder import (
    build_collection,
    list_asset_metadata,
    list_input_files,
    list_stac_items,
    load_collection,
    validate_collection,
)

containing_folder = Path(__file__).parent

# Collection configuration
catalog_version = "v0.2"
collection_config_path = containing_folder / "config-collection.json"

# Input Paths
tiff_input_path = Path("/dataCOPY/MTDA/MODIS/GLASS_FAPAR/tiff_collection_months_sd")
tiffs_glob = "*.tif"

# Output Paths
output_path = containing_folder / "results"
test_output_path = output_path / "test" / catalog_version
test_output_path.mkdir(parents=True, exist_ok=True)
publish_output_path = Path("/dataCOPY/MTDA/MODIS/GLASS_FAPAR/tiff_collection_months_sd/STAC_catalogs/v0.2/")
overwrite = True


# list input files
input_files = list_input_files(glob=tiffs_glob, input_dir=tiff_input_path, max_files=None)
print(f"Found {len(input_files)} input files. 5 first files:")
for i in input_files[:5]:
    print(i)


# list meta data
asset_metadata = list_asset_metadata(
    collection_config_path=collection_config_path, glob=tiffs_glob, input_dir=tiff_input_path, max_files=5
)
for k in asset_metadata:
    pprint.pprint(k.to_dict())


# list items
stac_items, failed_files = list_stac_items(
    collection_config_path=collection_config_path, glob=tiffs_glob, input_dir=tiff_input_path, max_files=0
)
print(f"Found {len(stac_items)} STAC items")
if failed_files:
    print(f"Failed files: {failed_files}")


print("First stac item:")
stac_items[0]


rmtree(test_output_path)


# # build grouped collection
# build_grouped_collections(
#     collection_config_path=collection_config_path,
#     glob=tiffs_glob,
#     input_dir=tiff_input_path,
#     output_dir=publish_output_path,
#     overwrite=overwrite,
# )

# build collection
build_collection(
    collection_config_path=collection_config_path,
    glob=tiffs_glob,
    input_dir=tiff_input_path,
    output_dir=publish_output_path,
    overwrite=overwrite,
)


# show collection
load_collection(collection_file=publish_output_path / "collection.json")


# validate collection
validate_collection(
    collection_file=publish_output_path / "collection.json",
)
