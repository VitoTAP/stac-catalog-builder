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

from pathlib import Path
from typing import List, Optional

from stacbuilder import (
    AssetMetadata,
    AssetMetadataPipeline,
    CollectionConfig,
    FileCollectorConfig,
)
from stacbuilder.collector import GeoTiffMetadataCollector, IMetadataCollector


def build_collection(
    collection_id: Optional[str] = None,
    output_dir: Optional[Path] = None,
) -> None:
    """Build a STAC collection for one of the collections in HRL VPP (OpenSearch)."""

    collection_config_path = Path("./config-collection.json").expanduser().absolute()
    coll_cfg = CollectionConfig.from_json_file(collection_config_path)
    file_coll_cfg = FileCollectorConfig(input_dir=Path("/data/MTDA/AgERA5/2022/"), glob="*/AgERA5_*.tif", max_files=100)

    if output_dir and not isinstance(output_dir, Path):
        output_dir = Path(output_dir).expanduser().absolute()

    collector = GeoTiffMetadataCollector.from_config(collection_config=coll_cfg, file_coll_cfg=file_coll_cfg)

    if output_dir and not isinstance(output_dir, Path):
        output_dir = Path(output_dir).expanduser().absolute()
        output_dir = output_dir / collection_id

    type_mapping = {
        "temperature-max": "2m_temperature_max",
        "temperature-min": "2m_temperature_min",
        "temperature-mean": "2m_temperature_mean",
        "dewpoint-temperature": "dewpoint_temperature_mean",
        "precipitation-flux": "total_precipitation",
        "solar-radiation-flux": "solar_radiation_flux",
        "vapour-pressure": "vapour_pressure",
        "wind-speed": "wind_speed",
    }

    class CustomCollector(IMetadataCollector):
        def has_collected(self) -> bool:
            return collector.has_collected()

        def reset(self):
            collector.reset()

        @property
        def metadata_list(self) -> List[AssetMetadata]:
            metadata_list = collector.metadata_list

            def update_metadata(metadata: AssetMetadata) -> AssetMetadata:
                metadata.asset_type = type_mapping.get(metadata.asset_type, metadata.asset_type)
                metadata.item_id = "agera5" + metadata.item_id
                metadata.href = metadata.href.replace("/data/MTDA", "https://services.terrascope.be/download")
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


build_collection("worldcover", "./STAC_wip")
