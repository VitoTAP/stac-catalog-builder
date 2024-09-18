"""
weed habitat maps stac generation

"""

from upath import UPath
from pathlib import Path
from typing import Optional, List

from stacbuilder import CollectionConfig, FileCollectorConfig, GeoTiffPipeline, AssetMetadataPipeline, AssetMetadata
from stacbuilder.collector import GeoTiffMetadataCollector, IMetadataCollector


def build_collection(
    collection_id: Optional[str] = None,
    output_dir: Optional[Path] = None,

) -> None:
    """Build a STAC collection for one of the collections in HRL VPP (OpenSearch)."""

    collection_config_path = Path("./config-collection.json").expanduser().absolute()
    coll_cfg = CollectionConfig.from_json_file(collection_config_path)
    file_coll_cfg = FileCollectorConfig(input_dir=UPath("/data/open/prepared_SK_alpha0_habitat-maps"), glob="*.tif", max_files=100)

    if output_dir and not isinstance(output_dir, Path):
        output_dir = Path(output_dir).expanduser().absolute()

    collector = GeoTiffMetadataCollector.from_config(collection_config=coll_cfg, file_coll_cfg=file_coll_cfg)

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
                metadata.href = metadata.href.replace("/data/open","https://services.terrascope.be/download/open")
                return metadata
            return [update_metadata(m) for m in metadata_list ]

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
        parts = item.id.split("_")

        return item

    pipeline.item_postprocessor = process_item
    pipeline.build_collection()

build_collection("habitat","./STAC_wip")