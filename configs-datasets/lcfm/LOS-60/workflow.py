# import pprint
from pathlib import Path

import pystac
from loguru import logger

# run pip install -e . in the root directory to install this package
from stacbuilder import (  # list_asset_metadata,; list_input_files,; list_stac_items,
    build_collection,
    validate_collection,
)

workflow_base_path = Path(__file__).parent.resolve()

# Write out debug logs to a file
logger.add(
    workflow_base_path / "debug.log",
    level="DEBUG",
)

# Collection configuration
collection_config_path = workflow_base_path / "config-collection.json"

# Input Paths
year = "*"
tiff_input_path = Path("/vitodata/vegteam_los/")
assert tiff_input_path.exists(), f"Path does not exist: {tiff_input_path}"
tiffs_glob = f"{year}/products/LCFM/LOS/v100/tiles_utm/*/*/*/*/*/*_PROBS_60M.tif"

# Output Paths
output_path = workflow_base_path / "results"
print(f"Output path: {output_path}")


def item_postprocessor(item: pystac.Item) -> pystac.Item:
    tileId = item.id.split("_")[-3]
    item.properties["tileId"] = tileId

    item.properties["product_version"] = "v100"
    return item


build_collection(
    collection_config_path=collection_config_path,
    glob=tiffs_glob,
    input_dir=tiff_input_path,
    output_dir=output_path,
    link_items=False,
    item_postprocessor=item_postprocessor,
    single_asset_per_item=True,
)

validate_collection(
    collection_file=output_path / "collection.json",
)
