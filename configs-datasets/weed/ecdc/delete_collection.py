import requests
from owslib.ogcapi.records import Records
from owslib.util import Authentication
import json
from pathlib import Path

APEX_CLIENT_ID = ""
APEX_CLIENT_SECRET = ""
APEX_TOKEN_URL = ""
CATALOGUE_URL = "https://catalogue.weed.apex.esa.int"
CONFIG_BASE = Path("auxiliary").expanduser().absolute()
COLLECTION_JSON = CONFIG_BASE / "collection.json"


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


def delete_collection(auth: BearerAuth, coll_id: str):
    """DELETE the collection from the catalogue."""
    url = f"{CATALOGUE_URL}/collections/{coll_id}"
    resp = requests.delete(url, auth=auth)
    if resp.status_code == 204:
        print(f"Collection {coll_id} deleted successfully")
    else:
        print(f"Failed to delete collection: HTTP {resp.status_code}\n{resp.text}")


# auth
auth = get_bearer_auth()
print(f"Bearer token: {auth.token}")

coll = json.loads(COLLECTION_JSON.read_text())
coll_id = coll["id"]
print(coll_id)
delete_collection(auth, coll_id)
