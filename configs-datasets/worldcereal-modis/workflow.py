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
collection_config_path = Path(__file__).parent.resolve() / "config-collection.json"

# Input Paths
tiff_input_path = Path("/vitodata/worldcereal/data/yield_module/ukraine/2020/")
assert tiff_input_path.exists(), f"Path does not exist: {tiff_input_path}"
tiffs_glob = "*.tif"

# Output Paths
output_path = Path(__file__).parent.resolve() / "stac"

# build collection
build_collection(
    collection_config_path=collection_config_path,
    glob=tiffs_glob,
    input_dir=tiff_input_path,
    output_dir=output_path,
    link_items=False,
)

# validate collection
validate_collection(
    collection_file=output_path / "collection.json",
)
