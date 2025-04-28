import requests
from owslib.ogcapi.records import Records
from owslib.util import Authentication
import json
from pathlib import Path
from stacbuilder import upload_to_stac_api

# ------------------------------------------------------------------------------
# Configuration (pull from environment or prompt)
# ------------------------------------------------------------------------------

APEX_CLIENT_ID = ""
APEX_CLIENT_SECRET = ""
APEX_TOKEN_URL = ""
CATALOGUE_URL = "https://catalogue.weed.apex.esa.int"
CONFIG_BASE = Path("era5land").expanduser().absolute()
COLLECTION_JSON = CONFIG_BASE / "collection.json"
ITEMS_BASE = CONFIG_BASE / "ERA5LAND-V01"

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
        "grant_type": "client_credentials",
        "client_id": APEX_CLIENT_ID,
        "client_secret": APEX_CLIENT_SECRET,
        "scope": "openid roles",
    }
    resp = requests.post(APEX_TOKEN_URL, data=data)
    resp.raise_for_status()
    token = resp.json()["access_token"]
    return BearerAuth(token)


auth = get_bearer_auth()
print(f"Bearer token: {auth.token}")


# ------------------------------------------------------------------------------
# STAC Collection + Item Ingestion
# ------------------------------------------------------------------------------


def create_collection(auth: BearerAuth) -> str:
    """POST the collection.json to the catalogue, returns collection ID."""
    coll = json.loads(COLLECTION_JSON.read_text())
    coll.setdefault("_auth", {"read": ["anonymous"], "write": ["stac-admin-prod"]})

    url = f"{CATALOGUE_URL}/collections"
    resp = requests.post(url, auth=auth, json=coll)
    if resp.status_code == 201:
        coll_id = resp.json()["id"]
        print(f"Collection created: {coll_id}")
        return coll_id
    elif resp.status_code == 400:
        raise RuntimeError("Collection validation failed")
    else:
        resp.raise_for_status()


coll_id = create_collection(auth)


# %%
# Add items
def ingest_all_items(auth: BearerAuth, coll_id: str, items_base: Path):
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
            item = json.loads(item_file.read_text())
            item["collection"] = coll_id
        except Exception as e:
            print(f"Failed to load {item_file}: {e}")
            continue

        url = f"{CATALOGUE_URL}/collections/{coll_id}/items"
        resp = requests.post(url, auth=auth, json=item)
        if resp.ok:
            print(f"  ✓ {item_file.relative_to(items_base)}")
        else:
            print(f"  ✗ {item_file.relative_to(items_base)} → {resp.status_code} {resp.text}")


# after create_collection() returns coll_id:
ingest_all_items(auth, coll_id, ITEMS_BASE)
