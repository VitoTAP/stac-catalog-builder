import shapely
from pathlib import Path
from typing import Optional, List
import pystac

from stacbuilder import CollectionConfig, FileCollectorConfig, GeoTiffPipeline, AssetMetadataPipeline, AssetMetadata
from stacbuilder.boundingbox import BoundingBox
from stacbuilder.collector import GeoTiffMetadataCollector, IMetadataCollector


def build_collection(
    collection_id: Optional[str] = None,
    output_dir: Optional[Path] = None,

) -> None:
    """Build a STAC collection for the ESA CCI Biomass AGB dataset"""

    collection_config_path = Path("./config-collection.json").expanduser().absolute()
    coll_cfg = CollectionConfig.from_json_file(collection_config_path)
    file_coll_cfg = FileCollectorConfig(input_dir=Path("/data/open/luisa/esacci_biomass_agb"), glob="*.tif", max_files=200000)

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
                metadata.geometry_lat_lon = shapely.box(-180,-90,180,90)
                metadata.bbox_lat_lon = BoundingBox.from_list([-180,-90,180,90], 4326)

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

    # def process_item(item: pystac.Item) -> pystac.Item:
    #     def local_to_s3_path(local_path: str) -> str:
    #         filename = local_path.split('/')[-1]
    #         s3_path = 's3://agera_monthly_v2/' + filename
    #         return s3_path

    #     for asset in item.assets.values():
    #         asset.href = local_to_s3_path(asset.href)

    #     return item

    # pipeline.item_postprocessor = process_item
    pipeline.build_collection()

build_collection("ESACCI_BIOMASS_AGB","./STAC")
