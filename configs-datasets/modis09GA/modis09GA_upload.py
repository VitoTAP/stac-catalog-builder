import os
from pathlib import Path

import liboidcagent as agent
import requests

from stacbuilder import Uploader


class BearerAuth(requests.auth.AuthBase):
    def __init__(self, token):
        self.token = token

    def __call__(self, r):
        r.headers["authorization"] = "Bearer " + self.token
        return r


# run `oidc-agent-service use` to print required env vars
# oidc-gen --pub terrascope --flow=device --client-id=public --iss=https://sso.terrascope.be/auth/realms/terrascope
os.environ["OIDC_SOCK"] = "/tmp/oidc-agent-service-13750/oidc-agent.sock"
token, issuer, expires_at = agent.get_token_response("terrascope")

auth = BearerAuth(token)

uploader = Uploader.create_uploader(
    "https://stac.openeo.vito.be/",
    auth,
    {"_auth": {"read": ["anonymous"], "write": ["stac-openeo-admin", "stac-openeo-editor"]}},
)
collection_path = Path("STAC_wip/collection.json")
uploader.upload_items(collection_path, items=collection_path.parent)
