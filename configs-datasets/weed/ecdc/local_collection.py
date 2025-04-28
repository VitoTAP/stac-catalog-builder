from upath import UPath
from pathlib import Path
from pystac import Collection
from os import environ
from typing import List
from stacbuilder import CollectionConfig, FileCollectorConfig, AssetMetadataPipeline, AssetMetadata
from stacbuilder.collector import GeoTiffMetadataCollector, IMetadataCollector

environ["AWS_ACCESS_KEY_ID"] = ""
environ["AWS_SECRET_ACCESS_KEY"] = ""
environ["AWS_ENDPOINT_URL_S3"] = "https://s3.waw3-1.cloudferro.com"
environ["AWS_S3_ENDPOINT"] = "s3.waw3-1.cloudferro.com"
environ["AWS_VIRTUAL_HOSTING"] = "FALSE"
environ["AWS_DEFAULT_REGION"] = "default"
environ["CPL_VSIL_CURL_CHUNK_SIZE"] = "10485760"
environ["CURL_CA_BUNDLE"] = ""


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
    collector = GeoTiffMetadataCollector.from_config(collection_config=coll_cfg, file_coll_cfg=file_coll_cfg)

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
    return noofassets, output_path


# BUILD COLLECTION
# Define parameters
inputdir = UPath("s3://bucket/Europe/ERA5-Land/")
configfile = "era5land-config-collection.json"
filepattern = "*/*.tiff"
no_input_assets, output_path = buildcollection_locally(inputdir, configfile, filepattern)

# VALIDATE COLLECTION
# stac validation
print("VALIDATION:")
collection = Collection.from_file(output_path / "collection.json")
noitems = collection.validate_all()
print(f"  Number of items created in collection is {noitems}")
# if the number of assets are proper
noassets = 0
for _i in collection.get_items():
    noassets += len(_i.assets)
print(f"  Number of assets in collection is {noassets} and input is {no_input_assets}")
