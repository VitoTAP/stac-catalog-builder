from pathlib import Path

from stacbuilder import (
    build_collection,
    validate_collection,
)

# Collection configuration
collection_config_path = Path(__file__).parent.resolve() / "config-collection.json"

# Input Paths
tiff_input_path = Path("/data/worldcereal_data/test_modis/2020/")
assert tiff_input_path.exists(), f"Path does not exist: {tiff_input_path}"
tiffs_glob = "*.tif"

# Output Paths
output_path = Path('/data/users/Public/vincent.verelst/modis_stac/')  # Path(__file__).parent.resolve() / "stac"

# build collection
build_collection(
    collection_config_path=collection_config_path,
    glob=tiffs_glob,
    input_dir=tiff_input_path,
    output_dir=output_path,
    link_items=True,
)

# validate collection
validate_collection(
    collection_file=output_path / "collection.json",
)
