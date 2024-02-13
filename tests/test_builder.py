"""Tests for the stacbuilder.builder module

Classes we really need to add coverage for:

TODO: need more coverage for AssetMetadataPipeline
TODO: need more coverage for GeoTiffPipeline
TODO: need coverage for MapMetadataToSTACItem
TODO: need coverage for MapGeoTiffToSTACItem
TODO: need coverage for STACCollectionBuilder
TODO: need coverage for GroupMetadataByYear and GroupMetadataByAttribute
TODO: need coverage for PostProcessSTACCollectionFile

Best to add unit tests in a bottom-up way.

"""
import json
from pathlib import Path
import pprint
from typing import List


import pytest
from pystac.collection import Collection


from stacbuilder.boundingbox import BoundingBox
from stacbuilder.builder import (
    AlternateHrefGenerator,
    AssetMetadataPipeline,
    GeoTiffPipeline,
    PostProcessSTACCollectionFile,
)
from stacbuilder.config import (
    AlternateHrefConfig,
    CollectionConfig,
    InputPathParserConfig,
)
from stacbuilder.exceptions import InvalidConfiguration
from stacbuilder.metadata import AssetMetadata, RasterMetadata, BandMetadata
from tests.conftest import MockMetadataCollector, MockPathParser


@pytest.fixture
def collection_test_config() -> CollectionConfig:
    """Collection configuration for use in pipeline fixtures."""

    data = {
        "collection_id": "foo-2023-v01",
        "title": "Foo collection",
        "description": "Description of Foo",
        "instruments": [],
        "keywords": ["foo", "bar", "oof"],
        "mission": [],
        "platform": [],
        "providers": [
            {
                "name": "Test EO Org",
                "roles": ["licensor", "processor", "producer"],
                "url": "https://www.test-eo-org.nowhere.to.be.found.xyz/",
            }
        ],
        "input_path_parser": InputPathParserConfig(
            classname="RegexInputPathParser",
            parameters={
                "regex_pattern": "observations_(?P<asset_type>[a-zA-Z0-9\\-]+)_(?P<datetime>\\d{4}-\\d{2}-\\d{2})\\.tif$"
            },
        ),
        "item_assets": {
            "2m-temp-monthly": {
                "title": "2m temperature",
                "description": "temperature 2m above ground (Kelvin)",
                "eo_bands": [
                    {"name": "2m_temp", "description": "temperature 2m above ground (Kelvin)", "data_type": "uint16"}
                ],
            },
            "tot-precip-monthly": {
                "title": "total precipitation",
                "description": "total precipitation per month (m)",
                "eo_bands": [
                    {"name": "tot_precip", "description": "total precipitation per month (m)", "data_type": "uint16"}
                ],
            },
        },
    }
    return CollectionConfig(**data)


def create_basic_asset_metadata(asset_path: Path) -> AssetMetadata:
    """Create a AssetMetadata with basic (fake) information.

    The relative_asset_path are expected to come from the fixture geotif_paths_relative or alike
    So they should look similar to the one below:

        2000/observations_2m-temp-monthly_2000-01-01.tif

    """

    md = AssetMetadata()
    path_parser = MockPathParser()

    md.asset_id = asset_path.stem
    asset_path_data = path_parser.parse(asset_path)

    prefix_length = len("observations_")
    end_length = len("_2000-01-01.tif")
    md.asset_type = asset_path.name[prefix_length:-end_length]

    md.item_id = asset_path_data["item_id"]

    md.asset_path = asset_path
    md.href = asset_path
    md.original_href = asset_path
    md.collection_id = asset_path_data["collection_id"]

    md.datetime = asset_path_data["datetime"]
    md.start_datetime = asset_path_data["start_datetime"]
    md.end_datetime = asset_path_data["end_datetime"]
    md.shape = (180, 240)
    md.tags = {"AREA_OR_POINT": "Area"}

    md.file_size = asset_path.stat().st_size

    bbox_dict = {"east": 240.0, "epsg": 4326, "north": 0.0, "south": 180.0, "west": 0.0}
    md.bbox_lat_lon = BoundingBox.from_dict(bbox_dict)
    md.bbox_projected = BoundingBox.from_dict(bbox_dict)
    md.transform = [1.0, 0.0, 0.0, 0.0, 1.0, 0.0]

    band = BandMetadata(data_type="float64", index=0, nodata=None)
    md.raster_metadata = RasterMetadata(shape=md.shape, bands=[band])
    return md


@pytest.fixture
def basic_asset_metadata(data_dir) -> AssetMetadata:
    # TODO: maybe better to save a few AssetMetadata and their STAC items to JSON and load them here
    return create_basic_asset_metadata(data_dir / "observations_2m-temp-monthly_2000-01-01.tif")


@pytest.fixture
def basic_asset_metadata_list(geotiff_paths) -> List[AssetMetadata]:
    return [create_basic_asset_metadata(f) for f in geotiff_paths]


@pytest.fixture
def basic_asset_metadata_list_small_set(geotiff_paths_small_set) -> List[AssetMetadata]:
    return [create_basic_asset_metadata(f) for f in geotiff_paths_small_set]


@pytest.fixture
def metadata_collector_basic_assets(basic_asset_metadata_list) -> MockMetadataCollector:
    return MockMetadataCollector(basic_asset_metadata_list)


@pytest.fixture
def asset_metadata_pipeline(
    metadata_collector_basic_assets, collection_config_from_file, tmp_path
) -> AssetMetadataPipeline:
    return AssetMetadataPipeline.from_config(
        metadata_collector=metadata_collector_basic_assets,
        collection_config=collection_config_from_file,
        output_dir=tmp_path,
    )


class TestGeoTiffPipeline:
    def test_get_input_files(self, geotiff_pipeline: GeoTiffPipeline, geotiff_paths: List[Path]):
        input_files = list(geotiff_pipeline.get_input_files())

        assert sorted(input_files) == sorted(geotiff_paths)

    def test_get_asset_metadata(self, geotiff_pipeline: GeoTiffPipeline, basic_asset_metadata_list: List[Path]):
        metadata_list = list(geotiff_pipeline.get_asset_metadata())

        sorted_actual_metadata_list = sorted(metadata_list)
        sorted_expected_metadata_list = sorted(basic_asset_metadata_list)

        expected_dicts = [e.to_dict() for e in sorted_expected_metadata_list]
        actual_dicts = [a.to_dict() for a in sorted_actual_metadata_list]

        for i, expected_md in enumerate(sorted_expected_metadata_list):
            pprint.pprint(expected_md.get_differences(sorted_actual_metadata_list[i]))

        assert expected_dicts == actual_dicts
        assert sorted_actual_metadata_list == sorted_expected_metadata_list

    def test_build_collection(self, geotiff_pipeline: GeoTiffPipeline):
        pipeline = geotiff_pipeline
        assert pipeline.collection is None

        pipeline.build_collection()

        assert pipeline.collection is not None
        assert pipeline.collection_file is not None
        assert pipeline.collection_file.exists()
        Collection.validate_all(pipeline.collection)

        collection = Collection.from_file(pipeline.collection_file)
        collection.validate_all()

    def test_build_grouped_collection(self, geotiff_pipeline_grouped: GeoTiffPipeline):
        pipeline = geotiff_pipeline_grouped
        assert pipeline.collection is None

        pipeline.build_grouped_collections()

        assert pipeline.collection_groups is not None
        assert pipeline.collection is None

        for coll in pipeline.collection_groups.values():
            coll_path = Path(coll.self_href)
            coll_path.exists()

            collection = Collection.from_file(coll_path)
            collection.validate_all()


class TestAssetMetadataPipeline:
    def test_collect_metadata(self, asset_metadata_pipeline: AssetMetadataPipeline, basic_asset_metadata_list):
        metadata_list = list(asset_metadata_pipeline.get_metadata())
        assert metadata_list == basic_asset_metadata_list

    def test_collect_stac_items(self, asset_metadata_pipeline: AssetMetadataPipeline):
        stac_items = list(asset_metadata_pipeline.collect_stac_items())
        assert len(stac_items) == 4

        for item in stac_items:
            len(item.assets) == 3

        # Validation should not raise any exceptions of type pystac.errors.STACValidationError
        for item in stac_items:
            item.validate()

    def test_build_collection(self, asset_metadata_pipeline: AssetMetadataPipeline):
        assert asset_metadata_pipeline.collection is None

        asset_metadata_pipeline.build_collection()

        assert asset_metadata_pipeline.collection is not None
        assert asset_metadata_pipeline.collection_file is not None
        assert asset_metadata_pipeline.collection_file.exists()
        Collection.validate_all(asset_metadata_pipeline.collection)

        # Validation should not raise any exceptions of type pystac.errors.STACValidationError
        collection = Collection.from_file(asset_metadata_pipeline.collection_file)
        collection.validate_all()

    def test_build_grouped_collection(self, asset_metadata_pipeline: AssetMetadataPipeline):
        assert asset_metadata_pipeline.collection is None

        asset_metadata_pipeline.build_grouped_collections()

        assert asset_metadata_pipeline.collection_groups is not None
        assert asset_metadata_pipeline.collection is None

        for coll in asset_metadata_pipeline.collection_groups.values():
            coll_path = Path(coll.self_href)
            coll_path.exists()

            # Validation should not raise any exceptions of type pystac.errors.STACValidationError
            collection = Collection.from_file(coll_path)
            collection.validate_all()


class TestAlternateLinksGenerator:
    @pytest.fixture
    def simple_asset_metadata(self) -> AssetMetadata:
        """A very simple AssetMetadata with minimal data"""
        asset_md = AssetMetadata()
        asset_md.asset_id = "asset123"
        asset_md.item_id = "item456"
        asset_md.collection_id = "collection789"
        asset_md.asset_path = Path("/data/collection789/item456/asset123.tif")

        return asset_md

    def test_it_registers_callbacks(self):
        alternate_generator = AlternateHrefGenerator()

        def fake_call_back(asset_md: AssetMetadata) -> str:
            return f"foo://bar/{asset_md.asset_id}"

        assert alternate_generator.has_alternate_key("FOO") is False
        alternate_generator.register_callback("FOO", fake_call_back)
        assert alternate_generator.has_alternate_key("FOO") is True

        assert alternate_generator._callbacks["FOO"] is fake_call_back

    def test_get_alternate_href_for(self, simple_asset_metadata):
        alternate_generator = AlternateHrefGenerator()

        def fake_call_back(asset_md: AssetMetadata) -> str:
            return f"foo://bar/{asset_md.asset_id}"

        alternate_generator.register_callback("FOO", fake_call_back)

        alternate_href = alternate_generator.get_alternate_href_for("FOO", simple_asset_metadata)
        assert alternate_href == "foo://bar/asset123"

    def test_get_alternates(self, simple_asset_metadata):
        alternate_generator = AlternateHrefGenerator()

        def fake_call_back(asset_md: AssetMetadata) -> str:
            return f"foo://bar/{asset_md.asset_id}"

        alternate_generator.register_callback("FOO", fake_call_back)

        alternates = alternate_generator.get_alternates(simple_asset_metadata)

        assert alternates == {"alternate": {"FOO": {"href": "foo://bar/asset123"}}}

    def test_mep(self, simple_asset_metadata):
        alternate_generator = AlternateHrefGenerator()

        assert alternate_generator.has_alternate_key("MEP") is False
        alternate_generator.add_MEP()
        assert alternate_generator.has_alternate_key("MEP") is True

        alternates = alternate_generator.get_alternates(simple_asset_metadata)

        assert alternates == {"alternate": {"MEP": {"href": "/data/collection789/item456/asset123.tif"}}}

    def test_S3_only_bucket(self, simple_asset_metadata):
        alternate_generator = AlternateHrefGenerator()

        assert alternate_generator.has_alternate_key("S3") is False
        alternate_generator.add_basic_S3(s3_bucket="test-bucket")
        assert alternate_generator.has_alternate_key("S3") is True

        alternates = alternate_generator.get_alternates(simple_asset_metadata)
        assert alternates == {"alternate": {"S3": {"href": "s3://test-bucket/data/collection789/item456/asset123.tif"}}}

    def test_S3_with_root_path(self, simple_asset_metadata):
        alternate_generator = AlternateHrefGenerator()

        assert alternate_generator.has_alternate_key("S3") is False
        alternate_generator.add_basic_S3(s3_bucket="test-bucket", s3_root_path="test/data-root/path")
        assert alternate_generator.has_alternate_key("S3") is True

        alternates = alternate_generator.get_alternates(simple_asset_metadata)
        assert alternates == {
            "alternate": {
                "S3": {"href": "s3://test-bucket/test/data-root/path/data/collection789/item456/asset123.tif"}
            }
        }

    def test_MEP_and_S3(self, simple_asset_metadata):
        alternate_generator = AlternateHrefGenerator()

        assert alternate_generator.has_alternate_key("MEP") is False
        assert alternate_generator.has_alternate_key("S3") is False

        alternate_generator.add_MEP()
        alternate_generator.add_basic_S3("test-bucket")
        assert alternate_generator.has_alternate_key("MEP") is True
        assert alternate_generator.has_alternate_key("S3") is True

        alternates = alternate_generator.get_alternates(simple_asset_metadata)
        assert alternates == {
            "alternate": {
                "MEP": {"href": "/data/collection789/item456/asset123.tif"},
                "S3": {"href": "s3://test-bucket/data/collection789/item456/asset123.tif"},
            }
        }

    def test_MEP_and_S3_with_root_path(self, simple_asset_metadata):
        alternate_generator = AlternateHrefGenerator()

        assert alternate_generator.has_alternate_key("MEP") is False
        assert alternate_generator.has_alternate_key("S3") is False

        alternate_generator.add_MEP()
        alternate_generator.add_basic_S3(s3_bucket="test-bucket", s3_root_path="test/data-root/path")

        assert alternate_generator.has_alternate_key("MEP") is True
        assert alternate_generator.has_alternate_key("S3") is True

        alternates = alternate_generator.get_alternates(simple_asset_metadata)
        assert alternates == {
            "alternate": {
                "MEP": {"href": "/data/collection789/item456/asset123.tif"},
                "S3": {"href": "s3://test-bucket/test/data-root/path/data/collection789/item456/asset123.tif"},
            }
        }

    @pytest.mark.parametrize(
        "config",
        [
            None,
            AlternateHrefConfig(add_MEP=False, add_S3=False),
            AlternateHrefConfig(add_MEP=True, add_S3=False),
            AlternateHrefConfig(add_MEP=False, add_S3=True, s3_bucket="test-bucket"),
            AlternateHrefConfig(add_MEP=False, add_S3=True, s3_bucket="test-bucket", s3_root_path="test/root-path"),
            AlternateHrefConfig(add_MEP=True, add_S3=True, s3_bucket="test-bucket", s3_root_path="test/root-path"),
        ],
    )
    def test_from_config_adds_correct_callback(self, config):
        alt_href_gen = AlternateHrefGenerator.from_config(config)

        if config is None:
            assert alt_href_gen.has_alternate_key("MEP") is False
            assert alt_href_gen.has_alternate_key("S3") is False
        else:
            assert alt_href_gen.has_alternate_key("MEP") == config.add_MEP
            assert alt_href_gen.has_alternate_key("S3") == config.add_S3

    @pytest.mark.parametrize(
        "config",
        [
            AlternateHrefConfig(add_MEP=False, add_S3=True, s3_bucket=None),
            AlternateHrefConfig(add_MEP=False, add_S3=True, s3_bucket=""),
            AlternateHrefConfig(add_MEP=False, add_S3=True, s3_bucket=None, s3_root_path="test/root-path"),
        ],
    )
    def test_from_config_raises_invalidconfiguration_when_s3bucket_missing(self, config):
        with pytest.raises(InvalidConfiguration):
            AlternateHrefGenerator.from_config(config)


class TestPostProcessSTACCollectionFile:
    @pytest.fixture
    def collection_config_file(self, data_dir) -> Path:
        return data_dir / "config/config-test-collection.json"

    @pytest.fixture
    def collection_config_file_no_overrides(self, tmp_path, collection_config_file) -> CollectionConfig:
        temp_config_path: Path = tmp_path / "collection-config.json"
        temp_config = CollectionConfig.from_json_file(collection_config_file)
        temp_config.overrides = None
        temp_config_path.write_text(temp_config.model_dump_json())
        return temp_config_path

    def test_override_collection_components(self):
        overrides = {
            "simple-added-value": "simple",
            "key:A/key:B": "new value",
            "ACME": {"anvil": "heavy"},
            "the/answer/to/everything": 42,
        }

        post_processor = PostProcessSTACCollectionFile(collection_overrides=overrides)
        data = {
            "key:A": {"key:B": "to be replaced"},
            "ACME": "a fictitious manufacturer of anvils",
            "one": 1,
            "two": "two",
            "three": 3.0,
        }

        post_processor._override_collection_components(data)

        # Keys that should be overwritten or added.
        assert data["simple-added-value"] == "simple"
        assert data["key:A"] == {"key:B": "new value"}
        assert data["ACME"] == {"anvil": "heavy"}
        assert data["the"] == {"answer": {"to": {"everything": 42}}}

        # Keys that should be left unchanged.
        assert data["one"] == 1
        assert data["two"] == "two"
        assert data["three"] == 3.0

    def test_process_collection_applies_overrides(self, asset_metadata_pipeline, tmp_path):
        asset_metadata_pipeline.build_collection()

        assert asset_metadata_pipeline.collection_file.exists()

        overrides = {"level_1/level_2": {"test_key": "test_value"}}
        post_processor = PostProcessSTACCollectionFile(collection_overrides=overrides)

        collection_file = asset_metadata_pipeline.collection_file
        post_proc_dir = tmp_path / "post-processed"
        post_processed_coll_path = post_proc_dir / "collection.json"
        post_processor.process_collection(collection_file=collection_file, output_dir=post_proc_dir)

        # Check that the overrides were applied
        collection_as_dict = None
        with open(post_processed_coll_path, "r", encoding="utf8") as f:
            collection_as_dict = json.load(f)

        assert "level_1" in collection_as_dict
        assert "level_2" in collection_as_dict["level_1"]
        assert collection_as_dict["level_1"]["level_2"] == {"test_key": "test_value"}

    @pytest.mark.parametrize("overrides", [None, {}])
    def test_process_collection_only_copies_files_when_no_overrides(self, overrides, asset_metadata_pipeline, tmp_path):
        """There are no overrides to apply but the files and directories should be copied to the new output directory.
        The collection filess should have identical contents.
        """
        asset_metadata_pipeline.build_collection()

        assert asset_metadata_pipeline.collection_file.exists()

        post_processor = PostProcessSTACCollectionFile(collection_overrides=overrides)

        collection_file = asset_metadata_pipeline.collection_file
        post_proc_dir = tmp_path / "post-processed"
        post_processed_coll_path = post_proc_dir / "collection.json"
        post_processor.process_collection(collection_file=collection_file, output_dir=post_proc_dir)

        original_contents = json.loads(collection_file.read_text(encoding="utf8"))
        processed_contents = json.loads(post_processed_coll_path.read_text(encoding="utf8"))
        assert original_contents == processed_contents

    @pytest.mark.parametrize("overrides", [None, {}])
    def test_process_collection_is_noop_when_no_overrides_and_in_place(
        self, overrides, asset_metadata_pipeline, tmp_path
    ):
        """
        The files should be left alone in this case, because there are no overrides,
        and the post-processing should be done in-place (i.e. it won't be copied to a new directory).

        That means, before and after the postprocessing, both the files modification time
        and its contents should be identical.
        """
        asset_metadata_pipeline.build_collection()

        assert asset_metadata_pipeline.collection_file.exists()

        post_processor = PostProcessSTACCollectionFile(collection_overrides=overrides)

        collection_file: Path = asset_metadata_pipeline.collection_file
        original_contents = json.loads(collection_file.read_text(encoding="utf8"))
        stats_before = collection_file.stat()
        modified_time_before = stats_before.st_mtime_ns

        post_processor.process_collection(collection_file=collection_file, output_dir=None)

        stats_after = collection_file.stat()
        modified_time_after = stats_after.st_mtime_ns
        assert modified_time_after == modified_time_before

        processed_contents = json.loads(collection_file.read_text(encoding="utf8"))
        assert processed_contents == original_contents
