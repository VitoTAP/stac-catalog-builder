"""
This script depends on environment variables to be set. The following environment variables are required:
AWS_ACCESS_KEY_ID=yyy
AWS_ENDPOINT_URL=https://s3.waw3-1.cloudferro.com
AWS_S3_ENDPOINT=s3.waw3-1.cloudferro.com
AWS_SECRET_ACCESS_KEY=xxx
AWS_VIRTUAL_HOSTING=FALSE
AWS_DEFAULT_REGION=default
CPL_VSIL_CURL_CHUNK_SIZE=10485760

This script needs additional packages, required to fetch data from S3 via a standard pathlib interface:
s3fs
universal-pathlib
fsspec

"""

from pathlib import Path
from typing import List, Optional

import shapely
from upath import UPath

from stacbuilder import (
    AssetMetadata,
    AssetMetadataPipeline,
    CollectionConfig,
    FileCollectorConfig,
)
from stacbuilder.boundingbox import BoundingBox
from stacbuilder.collector import IMetadataCollector, MetadataCollector


def build_collection(
    collection_id: Optional[str] = None,
    output_dir: Optional[Path] = None,
) -> None:
    """Build a STAC collection for one of the collections in HRL VPP (OpenSearch)."""

    collection_config_path = Path("./config-collection.json").expanduser().absolute()
    coll_cfg = CollectionConfig.from_json_file(collection_config_path)
    file_coll_cfg = FileCollectorConfig(input_dir=UPath("s3://agera_monthly_v2/"), glob="*.tif", max_files=200000)

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
                metadata.geometry_lat_lon = shapely.box(-180, -90, 180, 90)
                metadata.bbox_lat_lon = BoundingBox.from_list([-180, -90, 180, 90], 4326)

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
        return item

    pipeline.item_postprocessor = process_item
    pipeline.build_collection()


build_collection("agera5_monthly", "./STAC_wip2")
