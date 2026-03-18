import pprint
from pathlib import Path

import pystac
import rasterio

# run pip install -e . in the root directory to install this package
from stacbuilder import *

# Collection configuration
catalog_version = "v1"
collection_config_path = Path(__file__).parent.resolve() / "config-collection.json"

# Input Paths
tiff_input_path = Path("/data/MTDA/LCFM/products/LCM-10/v100/tiles_latlon/3deg/")
assert tiff_input_path.exists(), f"Path does not exist: {tiff_input_path}"
tiffs_glob = "*/*/2020/*_MAP.tif"

# Output Paths
output_path = Path(__file__).parent.resolve() / "results"
test_output_path = output_path / "test" / catalog_version
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
    item.collection_id = "lcfm-lcm-10"

    # --- STAC v1.1: upgrade raster:bands to bands, move nodata/data_type to asset level ---
    map_asset = item.assets["MAP"]
    raster_bands = map_asset.extra_fields.pop("raster:bands", [])
    if raster_bands:
        band = raster_bands[0]
        map_asset.extra_fields["nodata"] = band.get("nodata")
        map_asset.extra_fields["data_type"] = band.get("data_type")
    map_asset.extra_fields["bands"] = [{"name": "MAP"}]
    # Remove the raster extension, add the new bands reference
    item.stac_extensions = [ext for ext in item.stac_extensions if "raster" not in ext]

    # --- Title and GSD ---
    local_href = map_asset.href
    item.properties["title"] = Path(local_href).stem
    item.properties["gsd"] = 10

    # --- Processing extension ---
    with rasterio.open(local_href) as ds:
        tags = ds.tags() or {}
    creation_time = tags.get("creation_time")
    item.properties["processing:datetime"] = creation_time
    item.properties["processing:version"] = "v100"
    item.stac_extensions.append("https://stac-extensions.github.io/processing/v1.2.0/schema.json")

    # --- Authentication extension ---
    item.properties["auth:schemes"] = {
        "oidc": {
            "type": "openIdConnect",
            "description": "Authenticate with Terrascope OpenID Connect",
            "openIdConnectUrl": "https://sso.terrascope.be/auth/realms/terrascope/.well-known/openid-configuration",
        }
    }
    item.stac_extensions.append("https://stac-extensions.github.io/authentication/v1.1.0/schema.json")
    map_asset.extra_fields["auth:refs"] = ["oidc"]

    # --- Alternate assets extension ---
    map_asset.extra_fields["alternate"] = {"local": {"href": "file://" + local_href}}
    item.stac_extensions.append("https://stac-extensions.github.io/alternate-assets/v1.2.0/schema.json")
    map_asset.href = "https://services.terrascope.be/download/" + local_href[11:]

    # --- Preview asset ---
    preview_url = (
        f"https://titiler.terrascope.be/collections/lcfm-lcm-10/items/{item.id}"
        f"/preview?assets=MAP&format=png&max_size=256&colormap_name=lcfm"
    )
    item.assets["preview"] = pystac.Asset(
        href=preview_url,
        media_type="image/png",
        title="Preview",
        extra_fields={
            "description": "Preview image",
            "proj:shape": [256, 256],
            "proj:code": None,
            "roles": ["thumbnail", "overview"],
        },
    )

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
