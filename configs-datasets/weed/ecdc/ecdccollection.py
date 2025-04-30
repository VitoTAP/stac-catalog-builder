from pystac import Collection
from upath import UPath

from .functions import (
    buildcollection_locally,
    check_collection_exists,
    get_bearer_auth,
    set_s3bucket_env,
    create_collection_url,
    check_collection_exists,
    ingest_all_items,
    get_datafrom_toml,
    delete_collection,
)

# read input toml file
data = get_datafrom_toml("config.toml")

# BUILD COLLECTION
# Set environment variables for S3 bucket access
set_s3bucket_env(data)
# Define parameters
coll_inputs = data["stacbucket"]
inputdir = UPath(coll_inputs["input_datadir"])
configfile = coll_inputs["input_config_json"]
filepattern = coll_inputs["filepattern"]
no_input_assets = buildcollection_locally(inputdir, configfile, filepattern)


# CHECK IF COLLECTION WAS CREATED AND FILES DO EXIST
data = check_collection_exists(data)

# VALIDATE COLLECTION
# stac validation
print("VALIDATION:")
# assume the collection consists of linked items
collection = Collection.from_file(data["weedstac"]["data"]["collection_json"])
noitems = collection.validate_all()
print(f"  Number of items created in collection is {noitems}")
# if the number of assets are proper
noassets = 0
for _i in collection.get_items():
    noassets += len(_i.assets)
print(f"  Number of assets in collection is {noassets} and input is {no_input_assets}")


# UPLOADING and DELETING
# authentication
auth = get_bearer_auth(data["weedstac"]["auth"])
print(f"Bearer token: {auth.token}")

# UPLOADING COLLECTION
stacdata = data["weedstac"]["data"]
coll_id = create_collection_url(auth, stacdata)
# upload data
ingest_all_items(
    auth, stacdata["CATALOGUE_URL"], stacdata["collectionname"], stacdata["collectionpath"]
)

# DELETE
stacdata = data["weedstac"]["data"]
url = stacdata["CATALOGUE_URL"] / "collections" / stacdata["collection"]
delete_collection(auth, url)
