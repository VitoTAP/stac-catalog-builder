"""
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
from stacbuilder.collector import IMetadataCollector, MetadataFromFileCollector


def build_collection(
    collection_id: Optional[str] = None,
    output_dir: Optional[Path] = None,
) -> None:
    """Build a STAC collection for one of the collections in HRL VPP (OpenSearch)."""

    collection_config_path = Path("./config-collection.json").expanduser().absolute()
    coll_cfg = CollectionConfig.from_json_file(collection_config_path)
    file_coll_cfg = FileCollectorConfig(input_dir=UPath("/data/open/agera5_10daily"), glob="*.tif", max_files=200000)

    if output_dir and not isinstance(output_dir, Path):
        output_dir = Path(output_dir).expanduser().absolute()

    collector = MetadataFromFileCollector.from_config(collection_config=coll_cfg, file_coll_cfg=file_coll_cfg)

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
                metadata.item_id = metadata.item_id.replace("openEO_", "agera5_10daily_")
                metadata.href = metadata.href.replace("/data", "https://services.terrascope.be/download")

                return metadata

            return [update_metadata(m) for m in metadata_list]

        def collect(self) -> None:
            collector.collect()

    pipeline: AssetMetadataPipeline = AssetMetadataPipeline.from_config(
        metadata_collector=CustomCollector(),
        collection_config=coll_cfg,
        output_dir=output_dir,
        overwrite=True,
        link_items=False,
    )

    def process_item(item):
        return item

    pipeline.item_postprocessor = process_item
    pipeline.build_collection()


build_collection("agera5_dekad_terrascope", "./STAC_wip")
