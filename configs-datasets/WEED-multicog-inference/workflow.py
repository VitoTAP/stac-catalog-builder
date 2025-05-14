

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
from pathlib import Path
import pprint
import os
import json
import s3fs

# run pip install -e . in the root directory to install this package
from stacbuilder import *


#%% Next, configure AWS/S3 credentials for the CloudFerro S3 endpoint:

os.environ["AWS_ACCESS_KEY_ID"] = "a167aca763204d7f93b60a0ff1770d72"
os.environ["AWS_SECRET_ACCESS_KEY"] = "xx"
os.environ["AWS_ENDPOINT_URL_S3"] = "https://s3.waw4-1.cloudferro.com"
os.environ["AWS_S3_ENDPOINT"] = "s3.waw4-1.cloudferro.com"
os.environ["AWS_VIRTUAL_HOSTING"] = "FALSE"
os.environ["AWS_DEFAULT_REGION"] = "default"
os.environ["CPL_VSIL_CURL_CHUNK_SIZE"] = "10485760"

# %% STEP 3: Define paths

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


#%% First we create a standard configuration file which contains the collection metadata 
# Create a base collection config JSON (single-band) and compute a path for the multi-band variant:

collection_config_path = Path("C:\Git_projects\stac-catalog-builder\configs-datasets\WEED-multicog-inference\config-collection.json")
multiband_collection_config_path = collection_config_path.with_name(collection_config_path.stem + "-multicog" + collection_config_path.suffix)

#%% 3. Automating EO & Raster Band Injection

# Typically the eo:bands and raster:bands sections are laborious to hand-edit when a single asset contains 1000+ bands.
#  Instead, pull them from a representative STAC item, here be combine them in the asset feature_cube. Title and Description are mandatory:
"""
# add the path to a reperesentative json metadata file
# Read STAC JSON from S3
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

print(f"Found {len(eo_bands)} EO bands and {len(raster_bands)} raster bands")

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
with multiband_collection_config_path.open("w", encoding="utf-8") as f:
    json.dump(cfg, f, indent=2, ensure_ascii=False)

"""
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



output_path = Path("C:/Git_projects/stac-catalog-builder/configs-datasets/WEED-multicog-inference/results")
overwrite = True

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

#now you still update the collection.sjon file if you wish to adjust the tmporal and spatial context

#%%  Publishing to APEX Catalogue

#Use client credentials to push the collection

import requests
import json
from pathlib import Path
from shapely.geometry import shape

#------------------------------------------------------------------------------
# Configuration (pull from environment or prompt)
#------------------------------------------------------------------------------

APEX_CLIENT_ID = "weed-catalogue-prod-m2m",
APEX_CLIENT_SECRET = "yy",
CATALOGUE_URL = "https://catalogue.weed.apex.esa.int"
APEX_TOKEN_URL = "https://auth.apex.esa.int/realms/apex/protocol/openid-connect/token"

COLLECTION_JSON = Path("C:/Git_projects/stac-catalog-builder/configs-datasets/WEED-multicog-inference/results/collection.json")
ITEMS_BASE = Path("C:/Git_projects/stac-catalog-builder/configs-datasets/WEED-multicog-inference/results/feature_cube/2024/01/01")

# ------------------------------------------------------------------------------
# Authentication Helper
# ------------------------------------------------------------------------------

class BearerAuth(requests.auth.AuthBase):
    """Attach a Bearer token to the Authorization header."""
    def __init__(self, token: str):
        self.token = token

    def __call__(self, r):
        r.headers["Authorization"] = f"Bearer {self.token}"
        return r

def get_bearer_auth() -> BearerAuth:
    """Obtain an OAuth2 client_credentials access token for the APEX IDP."""
    data = {
        "grant_type":    "client_credentials",
        "client_id":     APEX_CLIENT_ID,
        "client_secret": APEX_CLIENT_SECRET,
        "scope":         "openid roles",
    }
    resp = requests.post(APEX_TOKEN_URL, data=data)
    resp.raise_for_status()
    token = resp.json()["access_token"]
    return BearerAuth(token)



def create_collection(auth: BearerAuth) -> str:
    """POST the collection.json to the catalogue, returns collection ID."""
    coll = json.loads(COLLECTION_JSON.read_text())
    coll.setdefault("_auth", {
        "read":  ["anonymous"],
        "write": ["stac-admin-prod"]
    })

    url = f"{CATALOGUE_URL}/collections"
    resp = requests.post(url, auth=auth, json=coll)
    if resp.status_code == 201:
        coll_id = resp.json()["id"]
        print(f"Collection created: {coll_id}")
        return coll_id
    elif resp.status_code == 400:
        print(f"Collection creation failed: {resp.text}")
    
    else:
        print(f"Collection creation failed: {resp.text}")
        resp.raise_for_status()



def ingest_all_items(auth: BearerAuth, coll_id: str, items_base: Path):
    """
    Walks items_base/**/ and POSTs every JSON file as an item
    into the given collection. Simplifies geometry before upload.
    """
    items = list(items_base.rglob("*.json"))
    print(f"Found {len(items)} JSON files under {items_base}")
    
    for item_file in items:
        try:
            item = json.loads(item_file.read_text())
            item["collection"] = coll_id

        except Exception as e:
            print(f"Failed to process {item_file}: {e}")
            continue
        
        url = f"{CATALOGUE_URL}/collections/{coll_id}/items"
        resp = requests.post(url, auth=auth, json=item)
        if resp.ok:
            print(f"    → Ingested {item_file.relative_to(items_base)}")
        else:
            print(
                f"    ✗ {item_file.relative_to(items_base)} → "
                f"{resp.status_code} {resp.text}"

            )

def delete_collection(auth: BearerAuth, coll_id: str):
    """DELETE the collection from the catalogue."""
    url = f"{CATALOGUE_URL}/collections/{coll_id}"
    resp = requests.delete(url, auth=auth)
    if resp.status_code == 204:
        print(f"Collection {coll_id} deleted successfully")
    else:
        print(f"Failed to delete collection: HTTP {resp.status_code}\n{resp.text}")


auth = get_bearer_auth()
coll_id = create_collection(auth)
ingest_all_items(auth, coll_id, ITEMS_BASE)


# %%
import openeo

spatial_extent = {
    "west": 22.815838456868914,
    "south": 43.15626349767909,
    "east": 22.855838456868914,
    "north": 43.20626349767909,
    "crs": 4326
}


connection = openeo.connect("https://openeo.dev.waw3-1.openeo-int.v1.dataspace.copernicus.eu/").authenticate_oidc()
datacube = connection.load_stac(f"https://catalogue.weed.apex.esa.int/collections/alpha2-test-results",
                                spatial_extent=spatial_extent,
                                bands = ["Level1_class-0_habitat-N-40000", "Level3_class-T3_habitat-T35-80305"])



datacube.execute_batch('test.tiff')


#%%



