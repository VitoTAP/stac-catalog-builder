"""This module contains functions to build a STAC collection from GeoTIFF files,
and to upload the collection and its items to a STAC catalogue."""

from os import environ
from json import loads
from pathlib import Path
from typing import List, Dict
from tomllib import load
from requests import auth, post, delete
from stacbuilder import (
    CollectionConfig,
    FileCollectorConfig,
    AssetMetadataPipeline,
    AssetMetadata,
)
from stacbuilder.collector import GeoTiffMetadataCollector, IMetadataCollector


def get_datafrom_toml(tomlfile):
    with open(tomlfile, "rb") as f:
        data = load(f)
    config_json = Path(data["stacbuild"]["input_config_file"]).expanduser().absolute()
    if config_json.exists():
        df = loads(config_json.read_text())
        if "collection_id" in df.keys():
            data["weedstac"]["data"]["collectionname"] = df["collection_id"]
        else:
            print("No collection_id in config")
    else:
        print(f"Config file {config_json} does not exist")
    return data


def set_s3bucket_env(data):
    if "s3bucket" not in data.keys():
        print("No s3bucket in config")
        exit()
    s3bucket = data["s3bucket"]
    for key, value in s3bucket.items():
        if not value:
            raise ValueError(
                "No proper enviornmental variables defined to access s3bucket"
            )
        else:
            print(key, value)
            environ[key] = value


def buildcollection_locally(data_input_path, configfile, filepattern):
    # create a custom collector
    class CustomCollector(IMetadataCollector):
        def has_collected(self) -> bool:
            return collector.has_collected()

        def reset(self):
            collector.reset()

        @property
        def metadata_list(self) -> List[AssetMetadata]:
            metadata_list = collector.metadata_list

            def update_metadata(metadata: AssetMetadata) -> AssetMetadata:
                # hardcode the minor version
                if metadata.item_id is not None:
                    parts = metadata.item_id.split("_", 1)
                    metadata.item_id = parts[1] + "_V" + parts[0][3:]
                else:
                    print("Item id is None")
                return metadata

            return [update_metadata(m) for m in metadata_list]

        def collect(self) -> None:
            collector.collect()

    # Find tiff files and print
    matching_tiffs = list(data_input_path.glob(filepattern))
    noofassets = len(matching_tiffs)
    if noofassets == 0:
        print("There are no assets")
        exit()

    # Collection configuration
    collection_config_path = Path(configfile).expanduser().absolute()
    coll_cfg = CollectionConfig.from_json_file(collection_config_path)
    file_coll_cfg = FileCollectorConfig(input_dir=data_input_path, glob=filepattern)

    # Output Paths
    output_path = Path(coll_cfg.collection_id)
    print(f"Output path is {coll_cfg.collection_id}")
    if output_path and not isinstance(output_path, Path):
        output_path = Path(output_path).expanduser().absolute()

    # Define collector
    collector = GeoTiffMetadataCollector.from_config(
        collection_config=coll_cfg, file_coll_cfg=file_coll_cfg
    )

    # create pipeline
    pipeline: AssetMetadataPipeline = AssetMetadataPipeline.from_config(
        metadata_collector=CustomCollector(),
        collection_config=coll_cfg,
        output_dir=output_path,
        overwrite=True,
    )

    # postprocessor to add new properties into items
    # example asset version and others
    def add_properties(item):
        return item

    pipeline.item_postprocessor = add_properties
    pipeline.build_collection()
    return noofassets


# check if collection was created and files do exist
def check_collection_exists(data):
    output_path = Path(data["weedstac"]["data"]["collectionname"]).expanduser().absolute()
    if output_path.exists():
        collection_json = output_path / "collection.json"
        if not collection_json.exists():
            print(f"Collection JSON not found at {collection_json}")
            exit(1)
            # add to collection json link to data dict
        data["weedstac"]["data"]["collectionpath"] = output_path
        data["weedstac"]["data"]["collection_json"] = collection_json
    else:
        print("There is no collection in the given path")
        exit(1)
    return data



# load to stac
class BearerAuth(auth.AuthBase):
    """Attach a Bearer token to the Authorization header."""

    def __init__(self, token: str):
        self.token = token

    def __call__(self, r):
        r.headers["Authorization"] = f"Bearer {self.token}"
        return r


def get_bearer_auth(stacauth) -> BearerAuth:
    """Obtain an OAuth2 client_credentials access token for the APEX IDP."""
    data = {
        "grant_type": "client_credentials",
        "client_id": stacauth["APEX_CLIENT_ID"],
        "client_secret": stacauth["APEX_CLIENT_SECRET"],
        "scope": "openid roles",
    }
    resp = post(stacauth["APEX_TOKEN_URL"], data=data)
    resp.raise_for_status()
    token = resp.json()["access_token"]
    return BearerAuth(token)


def create_collection_url(auth: BearerAuth, stacdata: Dict):
    """POST the collection.json to the catalogue, returns collection ID."""
    # load the collection.json from the created collection.
    coll = loads(stacdata["collection_json"].read_text())
    coll.setdefault("_auth", {"read": ["anonymous"], "write": ["stac-admin-prod"]})

    # define url for collection
    url = f"{stacdata['CATALOGUE_URL']}/collections"
    resp = post(url, auth=auth, json=coll)
    if resp.status_code == 201:
        coll_id = resp.json()["id"]        # collection_id
        print(f"Collection created: {coll_id}")
        return coll_id
    elif resp.status_code == 400:
        raise RuntimeError("Collection validation failed")
    else:
        resp.raise_for_status()


# Add items
def ingest_all_items(auth: BearerAuth, CATALOGUE_URL, coll_id: str, items_base: Path):
    """
    Walks items_base/**/ and POSTs every JSON file as an item
    into the given collection.
    """
    items = list(items_base.rglob("*.json"))
    print(f"Found {len(items)} JSON files under {items_base}")

    for item_file in items:
        # skip the collection.json if it lives in the same tree
        if item_file.name == "collection.json":
            continue
        
        try:
            item = loads(item_file.read_text())
            item["collection"] = coll_id
        except Exception as e:
            print(f"Failed to load {item_file}: {e}")
            continue

        url = f"{CATALOGUE_URL}/collections/{coll_id}/items"
        resp = post(url, auth=auth, json=item)
        if resp.ok:
            print(f"  ✓ {item_file.relative_to(items_base)}")
        else:
            print(
                f"  ✗ {item_file.relative_to(items_base)} → {resp.status_code} {resp.text}"
            )


def delete_collection(auth: BearerAuth, url_collection: str):
    """DELETE the collection from the catalogue."""
    #     url = f"{CATALOGUE_URL}/collections/{coll_id}"
    resp = delete(url_collection, auth=auth)
    if resp.status_code == 204:
        print(f"Collection {url_collection.rsplit('/')[-1]} deleted successfully")
    else:
        print(f"Failed to delete collection: HTTP {resp.status_code}\n{resp.text}")
