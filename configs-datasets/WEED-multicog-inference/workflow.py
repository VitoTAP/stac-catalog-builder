

#%%
"""
This document describes the custom workflow for building a multiband STAC collection using the stacbuilder package
The process includes:

Manually setting up input file patterns and initial configuration

Automating extraction of EO and raster bands from a representative STAC item

Building the collection via the command line (to support asynchronous code)

Post-processing item HREFs to adjust S3 links

Publishing and cleaning up the collection via APEX Catalogue APIs

Final sanity check with OpenEO
"""



# %% 1. Environment Setup and Imports
# Start by installing the package and importing required modules:

from upath import UPath
import configparser
from pathlib import Path
import pprint
import os
import json
import s3fs

# run pip install -e . in the root directory to install this package
from stacbuilder import *


#%% Next, configure AWS/S3 credentials for the CloudFerro S3 endpoint:

config = configparser.ConfigParser()
if not (Path(__file__).parent / "weed.conf").exists():
    raise FileNotFoundError("Configuration file for S3 not found")
config.read(Path(__file__).parent / "weed.conf")

os.environ["AWS_ACCESS_KEY_ID"] = config["EUGrasslandwatch"]["access_key_id"]
os.environ["AWS_SECRET_ACCESS_KEY"] = config["EUGrasslandwatch"]["secret_access_key"]
os.environ["AWS_ENDPOINT_URL_S3"] = config["EUGrasslandwatch"]["endpoint_url"]
os.environ["AWS_S3_ENDPOINT"] = config["EUGrasslandwatch"]["endpoint"]
os.environ["AWS_VIRTUAL_HOSTING"] = "FALSE"
os.environ["AWS_DEFAULT_REGION"] = "default"
os.environ["CPL_VSIL_CURL_CHUNK_SIZE"] = "10485760"

collection_config_path = Path(__file__).parent.resolve() / "config-collection.json"
collection_config = CollectionConfig.from_json_file(collection_config_path)
output_path = Path(__file__).parent.resolve() / "results"

overwrite = True

# %%

# set the file input path and the tiff glob pattern.
# here we are using a glob pattern to match all TIFF files in the specified directory.

tiff_input_path = UPath(
    "s3://ecdc-waw4-1-ekqouvq3otv8hmw0njzuvo0g4dy0ys8r985n7dggjis3erkpn5o/"
    "results/alpha2/feature_cube/v101"
)
tiffs_glob = '2024/**/WEED_v101_features-cube_year2024_*.tif'

input_files = list_input_files(
    glob=tiffs_glob,
    input_dir=tiff_input_path,
    max_files=None
)
print(f"Found {len(input_files)} input files. 5 first files:")
for i in input_files[:5]: print(i) 



#%% Automating EO & Raster Band Injection

# Typically the eo:bands and raster:bands sections are laborious to hand-edit when a single asset contains 1000+ bands.
#  Instead, pull them from a representative STAC item, here be combine them in the asset feature_cube. Title and Description are mandatory:


stac_s3_uri = (
    "s3://ecdc-waw4-1-ekqouvq3otv8hmw0njzuvo0g4dy0ys8r985n7dggjis3erkpn5o/"
    "results/alpha2/feature_cube/v101/2024/32/"
    "WEED_v101_features-cube_year2024_32τMM03.tif.json"
)

fs = s3fs.S3FileSystem(anon=False)
with fs.open(stac_s3_uri, "r") as f:
    stac = json.load(f)

# Extract eo:bands and raster:bands
asset_key = next(iter(stac["assets"]))
asset_data = stac["assets"][asset_key]
eo_bands_raw = asset_data.get("eo:bands", [])
raster_bands_raw = asset_data.get("raster:bands", [])

# Add descriptions to eo_bands
eo_bands = []
for b in eo_bands_raw:
    name = b["name"]
    common_name = b.get("common_name")
    desc = f"{name} ({common_name}) band" if common_name else f"{name} band"
    eo_bands.append({
        "name": name,
        "common_name": common_name,
        "center_wavelength": b.get("wavelength_um"),
        "description": desc
    })

# Flatten raster band stats
raster_bands = [
    {
        "name": b["name"],
        **b.get("statistics", {})
    }
    for b in raster_bands_raw
]


# Load and modify the collection config
with collection_config_path.open("r", encoding="utf-8") as f:
    cfg = json.load(f)

item_assets = cfg.setdefault("item_assets", {})
asset = item_assets.setdefault("feature-cube", {})

# Add top-level title and description if missing
asset.setdefault("title", "Feature Cube Asset")
asset.setdefault("description", "Multi-band statistical features computed per pixel from EO bands.")

# Inject updated bands
asset["eo_bands"] = eo_bands
asset["raster:bands"] = raster_bands

# Save to a new config file
multiband_collection_config_path = collection_config_path.with_name(collection_config_path.stem + "-multicog" + collection_config_path.suffix)
with multiband_collection_config_path.open("w", encoding="utf-8") as f:
    json.dump(cfg, f, indent=2, ensure_ascii=False)


# %% Built the collection, this part needs to be ran from shell rather than from a notebook due to the usage of 
asset_metadata = list_asset_metadata(
    collection_config_path=multiband_collection_config_path,
    glob=tiffs_glob,
    input_dir=tiff_input_path,
    max_files=1
)
for k in asset_metadata: 
    pprint.pprint(k.to_dict())


def item_postprocessor(item):
    if "feature-cube" in item.assets:
        original_href = item.assets["feature-cube"].href
        updated_href = original_href.replace(
            "s3://ecdc-waw4-1-ekqouvq3otv8hmw0njzuvo0g4dy0ys8r985n7dggjis3erkpn5o/",
            "https://s3.waw4-1.cloudferro.com/swift/v1/ecdc-waw4-1-ekqouvq3otv8hmw0njzuvo0g4dy0ys8r985n7dggjis3erkpn5o/"
        )
        item.assets["feature-cube"].href = updated_href
    return item


build_collection(
    collection_config_path=multiband_collection_config_path,
    glob=tiffs_glob,
    input_dir=tiff_input_path,
    output_dir=output_path,
    overwrite=overwrite,
    link_items=False,
    item_postprocessor=item_postprocessor,
)

exit()


