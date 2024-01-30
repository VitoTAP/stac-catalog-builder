"""
CLI command-style functions

The functions below are helper functions to keep the CLI in __main__.py
as thin and dumb as reasonably possible.

We want to the logic out of the CLI, therefore we put it in these functions
and the CLI only does the argument parsing, via the click library.

The main advantage is that this style allows for unit tests on core
functionality of the CLI, and that is harder to do directly on the CLI.
"""

from pathlib import Path
import pprint

from pystac import Collection
from typing import Optional


from stacbuilder.builder import (
    AssetMetadataPipeline,
    FileCollector,
    GeoTiffPipeline,
    GeodataframeExporter,
    PostProcessSTACCollectionFile,
)
from stacbuilder.config import CollectionConfig, FileCollectorConfig
from stacbuilder.terracatalog import HRLVPPMetadataCollector


class CLICommands:
    """Putting the new versions of the command under this class for now to
    make switching between old and new easier for testing the conversion.
    This is temporary. Going to move the commands to a separate module.
    """

    @staticmethod
    def build_collection(
        collection_config_path: Path,
        glob: str,
        input_dir: Path,
        output_dir: Path,
        overwrite: bool,
        max_files: Optional[int] = -1,
        save_dataframe: Optional[bool] = False,
    ):
        """Build a STAC collection from a directory of geotiff files."""
        collection_config_path = Path(collection_config_path).expanduser().absolute()
        coll_cfg = CollectionConfig.from_json_file(collection_config_path)
        file_coll_cfg = FileCollectorConfig(input_dir=input_dir, glob=glob, max_files=max_files)
        pipeline = GeoTiffPipeline.from_config(
            collection_config=coll_cfg,
            file_coll_cfg=file_coll_cfg,
            output_dir=output_dir,
            overwrite=overwrite,
        )

        pipeline.build_collection()

        if save_dataframe:
            GeodataframeExporter.export_item_bboxes(pipeline.collection)

    @staticmethod
    def build_grouped_collections(
        collection_config_path: Path,
        glob: str,
        input_dir: Path,
        output_dir: Path,
        overwrite: bool,
        max_files: Optional[int] = -1,
        save_dataframe: Optional[bool] = False,
    ):
        """Build a multiple STAC collections from a directory of geotiff files,
        where each collection groups related STAC items.

        The default grouping is per year.
        """

        collection_config_path = Path(collection_config_path).expanduser().absolute()
        coll_cfg = CollectionConfig.from_json_file(collection_config_path)
        file_coll_cfg = FileCollectorConfig(input_dir=input_dir, glob=glob, max_files=max_files)
        pipeline = GeoTiffPipeline.from_config(
            collection_config=coll_cfg,
            file_coll_cfg=file_coll_cfg,
            output_dir=output_dir,
            overwrite=overwrite,
        )

        pipeline.build_grouped_collections()

        if save_dataframe:
            for collection in pipeline.collection_groups.values():
                GeodataframeExporter.export_item_bboxes(collection)

    @staticmethod
    def extract_item_bboxes(collection_file: Path):
        """Extract the bounding boxes of the STAC items in the collection."""
        collection = Collection.from_file(collection_file)
        GeodataframeExporter.export_item_bboxes(collection)

    @staticmethod
    def list_input_files(
        glob: str,
        input_dir: Path,
        max_files: Optional[int] = -1,
    ):
        """List the geotiff files that are found with the current configuration."""

        collector = FileCollector()
        collector.input_dir = Path(input_dir)
        collector.glob = glob
        collector.max_files = max_files
        collector.collect()

        for file in collector.input_files:
            print(file)

    @staticmethod
    def list_asset_metadata(
        collection_config_path: Path,
        glob: str,
        input_dir: Path,
        max_files: Optional[int] = -1,
        save_dataframe: bool = False,
    ):
        """Show the AssetMetadata objects generated for each geotiff file.

        This is used to test the conversion and check the configuration files.
        """

        collection_config_path = Path(collection_config_path).expanduser().absolute()
        coll_cfg = CollectionConfig.from_json_file(collection_config_path)
        file_coll_cfg = FileCollectorConfig(input_dir=input_dir, glob=glob, max_files=max_files)
        pipeline = GeoTiffPipeline.from_config(collection_config=coll_cfg, file_coll_cfg=file_coll_cfg)

        if pipeline.has_grouping:
            for group, metadata_list in sorted(pipeline.get_metadata_groups().items()):
                print(f"=== group={group} ===")
                print(f"   number of assets: {len(metadata_list)}")

                for meta in metadata_list:
                    report = {"group": group, "metadata": meta.to_dict(include_internal=True)}
                    pprint.pprint(report)
                    print()
                print()
        else:
            for meta in pipeline.get_metadata():
                pprint.pprint(meta.to_dict(include_internal=True))
                print()

        if save_dataframe:
            df = pipeline.get_metadata_as_geodataframe()
            # TODO: Want better directory to save geodata, maybe use save_dataframe as path instead of flag.
            out_dir = Path("tmp") / coll_cfg.collection_id / "visualization_list-assetmetadata"
            GeodataframeExporter.save_geodataframe(df, out_dir, "metadata_table")

    @staticmethod
    def list_stac_items(
        collection_config_path: Path,
        glob: str,
        input_dir: Path,
        max_files: Optional[int] = -1,
        save_dataframe: bool = False,
    ):
        """Show the STAC items that are generated for each geotiff file.

        This is used to test the conversion and check the configuration files.
        """

        collection_config_path = Path(collection_config_path).expanduser().absolute()
        coll_cfg = CollectionConfig.from_json_file(collection_config_path)

        file_coll_cfg = FileCollectorConfig(input_dir=input_dir, glob=glob, max_files=max_files)

        pipeline = GeoTiffPipeline.from_config(
            collection_config=coll_cfg, file_coll_cfg=file_coll_cfg, output_dir=None, overwrite=False
        )

        stac_items = list(pipeline.collect_stac_items())
        files = list(pipeline.get_input_files())
        num_itemst = len(stac_items)
        for i, item in enumerate(stac_items):
            if item:
                pprint.pprint(item.to_dict())
            else:
                file = files[i]
                print(
                    f"Received None for a STAC Item {i+1} of {num_itemst}. "
                    + f"Item could not be generated for file: {file}"
                )

        if save_dataframe:
            df = pipeline.get_stac_items_as_geodataframe()
            # TODO: Want better directory to save geodata, maybe use save_dataframe as path instead of flag.
            out_dir = Path("tmp") / coll_cfg.collection_id / "visualization_list-stac-items"
            GeodataframeExporter.save_geodataframe(df, out_dir, "stac_items")

    @staticmethod
    def postprocess_collection(
        collection_file: Path,
        collection_config_path: Path,
        output_dir: Optional[Path] = None,
    ):
        """Run only the post-processing step, on an existing STAC collection.

        Mainly intended to troubleshoot the postprocessing so you don't have to
        regenerate the entire set every time.
        """
        collection_config_path = Path(collection_config_path).expanduser().absolute()
        coll_cfg = CollectionConfig.from_json_file(collection_config_path)

        postprocessor = PostProcessSTACCollectionFile(collection_overrides=coll_cfg.overrides)
        postprocessor.process_collection(collection_file=collection_file, output_dir=output_dir)

    @staticmethod
    def load_collection(
        collection_file: Path,
    ):
        """Show the STAC collection in 'collection_file'."""
        collection = Collection.from_file(collection_file)
        pprint.pprint(collection.to_dict(), indent=2)

    @staticmethod
    def validate_collection(
        collection_file: Path,
    ):
        """Validate a STAC collection."""
        collection = Collection.from_file(collection_file)
        collection.validate_all()


class HRLVVPCliCommands:
    """Commands specifically for the HRL VPP pipeline."""

    @staticmethod
    def list_metadata():
        """Show the AssetMetadata objects that are generated for each VPP product.

        This is used to test the conversion and check the configuration files.
        """
        collector = HRLVPPMetadataCollector()
        COLLECTION_ID = "copernicus_r_3035_x_m_hrvpp-st_p_2017-now_v01"
        collector.collection_id = COLLECTION_ID
        collector.collect()

        for md in collector.metadata:
            pprint.pprint(md.to_dict())

    @staticmethod
    def list_stac_items(
        collection_config_path: Path,
        max_products: Optional[int] = -1,
    ):
        """Show the STAC items that are generated for each VPP product.

        This is used to test the conversion and check the configuration files.
        """
        collector = HRLVPPMetadataCollector()
        COLLECTION_ID = "copernicus_r_3035_x_m_hrvpp-st_p_2017-now_v01"
        collector.collection_id = COLLECTION_ID
        collector.max_products = max_products
        collector.collect()

        collection_config_path = Path(collection_config_path).expanduser().absolute()
        coll_cfg = CollectionConfig.from_json_file(collection_config_path)
        pipeline = AssetMetadataPipeline.from_config(
            metadata_collector=collector,
            collection_config=coll_cfg,
            output_dir=None,
            overwrite=False,
        )

        for item in list(pipeline.collect_stac_items()):
            pprint.pprint(item.to_dict())

    @staticmethod
    def build_hrlvpp_collection(
        collection_config_path: Path,
        output_dir: Path,
        overwrite: bool,
        max_products: Optional[int] = -1,
        # save_dataframe: Optional[bool] = False,
    ):
        """Build a STAC collection for one of the collections in HRL VPP (OpenSearch)."""

        collector = HRLVPPMetadataCollector()
        COLLECTION_ID = "copernicus_r_3035_x_m_hrvpp-st_p_2017-now_v01"
        collector.collection_id = COLLECTION_ID
        collector.max_products = max_products
        collector.collect()

        collection_config_path = Path(collection_config_path).expanduser().absolute()
        coll_cfg = CollectionConfig.from_json_file(collection_config_path)
        pipeline = AssetMetadataPipeline.from_config(
            metadata_collector=collector,
            collection_config=coll_cfg,
            output_dir=output_dir,
            overwrite=overwrite,
        )

        pipeline.from_config()
        pipeline.build_collection()

        # if save_dataframe:
        #     GeodataframeExporter.export_item_bboxes(pipeline.collection)
