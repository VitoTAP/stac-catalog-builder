import os

import liboidcagent as agent
import pystac
import requests


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


collection = pystac.Collection.from_file("STAC_wip/collection.json")


coll_dict = collection.to_dict()

default_auth = {"_auth": {"read": ["anonymous"], "write": ["stac-openeo-admin", "stac-openeo-editor"]}}

coll_dict.update(default_auth)

# requests.delete("https://stac.openeo.vito.be/collections/modis-10A1-061", auth=auth)
response = requests.post("https://stac.openeo.vito.be/collections", auth=auth, json=coll_dict)
# response = requests.put("https://stac.openeo.vito.be/collections/modis-09GA-061", auth=auth,json=coll_dict)
print(response)
