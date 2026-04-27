from pathlib import Path

import pystac
from loguru import logger
from upath import UPath

from stacbuilder import (
    build_collection,
    load_env,
    validate_collection,
)

CWD = Path(__file__).parent

logger.add(
    CWD / "build.log",
    level="DEBUG",
)

# Collection configuration
catalog_version = "v01"
collection_config_path = CWD / "config-collection.json"

load_env(CWD / ".env")

for year in range(2017, 2026):
    # Input Paths
    tiff_input_path = UPath(f"s3://hr-vpp-products-vpp-v01-{year}/CLMS/Pan-European/Biophysical/VPP/v01/{year}/")
    tiffs_glob = "*/*.tif"

    # Output Paths
    output_path = CWD / "results" / catalog_version

    def postprocess_item(item: pystac.Item) -> pystac.Item:
        # Example postprocessing: add a custom property to the item
        season = item.id[-2:]
        item.properties["season"] = season
        tileid = item.id.split("_")[3].split("-")[0][1:]
        item.properties["tileId"] = tileid
        return item

    # build collection
    build_collection(
        collection_config_path=collection_config_path,
        glob=tiffs_glob,
        input_dir=tiff_input_path,
        output_dir=output_path,
        item_postprocessor=postprocess_item,
        link_items=False,
    )

    # validate collection
    validate_collection(
        collection_file=output_path / "collection.json",
    )
