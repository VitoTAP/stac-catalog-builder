"""Build a STAC collection for spatial features from GeoTIFF files.
   Needs LCFM shapefile from https://github.com/VITO-RS-Vegetation/lcfm-shapefiles/ to match the tiles.
   Adjust the path in line 61
"""

from pathlib import Path
from stacbuilder import (
    CollectionConfig,
    FileCollectorConfig,
    AssetMetadataPipeline,
    validate_collection,
    AuthSettings,
    Settings,
    upload_to_stac_api,
)
from stacbuilder.collector import GeoTiffMetadataCollector, IMetadataCollector
import pystac
import geopandas as gpd


data_input_path = Path("/vitodata/vegteam_lcfm_openeo/projects/evoland/spatial-features/v008-m10-c84/blocks/").expanduser().absolute()
configfile = "spatial_features.json"
overwrite = False
filepattern = "*/*/*/*/*/*.tif"

# Find tiff files and print
matching_tiffs = list(data_input_path.glob(filepattern))
noofassets = len(matching_tiffs)
print(f"Found {noofassets} assets matching the pattern {filepattern} in {data_input_path}")
if noofassets == 0:
   print("There are no assets")
   exit()

# Collection configuration
collection_config_path = Path(configfile).expanduser().absolute()
coll_cfg = CollectionConfig.from_json_file(collection_config_path)
file_coll_cfg = FileCollectorConfig(input_dir=data_input_path, glob=filepattern)
coll_cfg.media_type = pystac.MediaType.COG
print(coll_cfg.media_type)
# Output Paths
output_path = Path(coll_cfg.collection_id)
print(f"Output path is {coll_cfg.collection_id}")
if output_path and not isinstance(output_path, Path):
    output_path = Path(output_path).expanduser().absolute()

# Define collector
collector = GeoTiffMetadataCollector.from_config(collection_config=coll_cfg, file_coll_cfg=file_coll_cfg)

# create pipeline
pipeline: AssetMetadataPipeline = AssetMetadataPipeline.from_config(
    metadata_collector=collector,
    collection_config=coll_cfg,
    output_dir=output_path,
    link_items=False,
    overwrite=overwrite,
)

# postprocessor to add new properties into items
# example asset version and others
# read tiles bounds
tilesfilename = "lcfm_shapefiles/data/LCFM_100p_S2-reduced-tiles.fgb" # Adjust the path as needed
tiles = gpd.read_file(tilesfilename)

def add_properties(item):
   # add grid extension
    ext = pystac.extensions.grid.GridExtension.ext(item, add_if_missing=True)
    dff = tiles[tiles.tile == item.id.split("_")[1]]
    # item dff.west.values[0]
    coords = item.properties["proj:bbox"]
    easting = int((coords[0] - dff.west.values[0])/10)
    northing = int((coords[1] - dff.south.values[0])/10)
    ext.apply(code=f"MGRS-{item.id.split('_')[1]}{easting:04}{northing:04}")
    return item

pipeline.item_postprocessor = add_properties
pipeline.build_collection()

# validate collection
validate_collection(
    collection_file=output_path / "collection.json",
)

# upload stac collection to STAC API
auth_settings = AuthSettings(
    enabled=True,
    interactive=False,
    token_url="https://sso.terrascope.be/auth/realms/terrascope/protocol/openid-connect/token",
    authorization_url="https://sso.terrascope.be/auth/realms/terrascope/protocol/openid-connect/auth",
    client_id="terracatalogueclient",
    username="your_username",  # Replace with your username
    password="your_password",  # Replace with your password
)
settings = Settings(
    auth=auth_settings,
    stac_api_url="https://stac.openeo.vito.be/",
    collection_auth_info={
        "_auth": {
            "read": ["anonymous"],
            "write": ["stac-openeo-admin", "stac-openeo-editor"]
        }
    },
    bulk_size=1000,  # Number of items to upload in a single request
)

upload_to_stac_api(
    collection_path=output_path / "collection.json",
    settings=settings,
)
