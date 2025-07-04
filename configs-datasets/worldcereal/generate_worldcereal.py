from pathlib import Path
from typing import List, Optional

from stacbuilder import (
    AssetMetadata,
    AssetMetadataPipeline,
    CollectionConfig,
    FileCollectorConfig,
)
from stacbuilder.collector import IMetadataCollector, MetadataCollector


def build_collection(
    collection_id: Optional[str] = None,
    output_dir: Optional[Path] = None,
) -> None:
    """Build a STAC collection for one of the collections in HRL VPP (OpenSearch)."""

    collection_config_path = Path("./config-collection.json").expanduser().absolute()
    coll_cfg = CollectionConfig.from_json_file(collection_config_path)
    file_coll_cfg = FileCollectorConfig(
        input_dir="/vitodata/worldcereal_data/MAP-v3/2021/tc-maize-main/maize/tiles_utm",
        glob="c*/2021_summer1_33117*11SPR.tif",
        max_files=100,
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
                metadata.item_id = metadata.asset_id.replace("_confidence", "").replace("_classification", "")
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
        parts = item.id.split("_")
        aez = parts[2]
        season = parts[1]
        tileID = parts[4]
        item.properties["AEZ"] = aez
        item.properties["season"] = season
        item.properties["tileID"] = tileID
        return item

    pipeline.item_postprocessor = process_item
    pipeline.build_collection()


build_collection("worldcereal_maize", "./STAC_wip")
