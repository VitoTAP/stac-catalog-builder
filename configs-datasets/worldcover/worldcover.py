"""
This script depends on environment variables to be set. The following environment variables are required:
AWS_ACCESS_KEY_ID=XXX
AWS_ENDPOINT_URL=https://eodata.dataspace.copernicus.eu
AWS_S3_ENDPOINT=eodata.dataspace.copernicus.eu
AWS_SECRET_ACCESS_KEY=XXX
AWS_VIRTUAL_HOSTING=FALSE
AWS_DEFAULT_REGION=default
CPL_VSIL_CURL_CHUNK_SIZE=10485760

This script needs additional packages, required to fetch data from S3 via a standard pathlib interface:
s3fs
universal-pathlib
fsspec

"""

import configparser
import os
from pathlib import Path
from typing import List, Optional

from upath import UPath

from stacbuilder import (
    AssetMetadata,
    AssetMetadataPipeline,
    CollectionConfig,
    FileCollectorConfig,
)
from stacbuilder.collector import IMetadataCollector, MetadataCollector


def set_var_conf(var_name: str, conf_file: str):
    if var_name not in os.environ:
        print(f"Setting {var_name} from {conf_file}")
        config = configparser.ConfigParser()
        if not Path(conf_file).exists():
            raise FileNotFoundError(f"Configuration file for {var_name} not found")
        config.read(conf_file)
        os.environ[var_name] = config["eodata"][var_name]


for var_name in [
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
    "AWS_ENDPOINT_URL",
    "AWS_S3_ENDPOINT",
    "AWS_VIRTUAL_HOSTING",
    "AWS_DEFAULT_REGION",
    "CPL_VSIL_CURL_CHUNK_SIZE",
]:
    set_var_conf(var_name, "./eodata.conf")


def build_collection(
    collection_id: Optional[str] = None,
    output_dir: Optional[Path] = None,
) -> None:
    """Build a STAC collection for one of the collections in HRL VPP (OpenSearch)."""

    collection_config_path = Path("./config-collection.json").expanduser().absolute()
    coll_cfg = CollectionConfig.from_json_file(collection_config_path)
    file_coll_cfg = FileCollectorConfig(
        input_dir=UPath("s3://eodata/auxdata/ESA_WORLD_COVER/2020/"), glob="*/*.tif", max_files=1000
    )

    if output_dir and not isinstance(output_dir, Path):
        output_dir = Path(output_dir).expanduser().absolute()

    collector = MetadataCollector.from_config(collection_config=coll_cfg, file_coll_cfg=file_coll_cfg)

    if output_dir and not isinstance(output_dir, Path):
        output_dir = Path(output_dir).expanduser().absolute()
        output_dir = output_dir / collection_id

    class CustomCollector(IMetadataCollector):
        def has_collected(self) -> bool:
            return collector.has_collected()

        def reset(self):
            collector.reset()

        @property
        def metadata_list(self) -> List[AssetMetadata]:
            metadata_list = collector.metadata_list

            def update_metadata(metadata: AssetMetadata) -> AssetMetadata:
                return metadata

            return [update_metadata(m) for m in metadata_list]

        def collect(self) -> None:
            collector.collect()

    pipeline: AssetMetadataPipeline = AssetMetadataPipeline.from_config(
        metadata_collector=CustomCollector(),
        collection_config=coll_cfg,
        output_dir=output_dir,
        link_items=False,
    )

    def process_item(item):
        # parts = item.id.split("_")

        return item

    pipeline.item_postprocessor = process_item
    pipeline.build_collection()


build_collection("worldcover", "./STAC_wip")
