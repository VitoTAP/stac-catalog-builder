"""Tests for the stacbuilder.builder module

Classes we really need to add coverage for:

TODO: need more coverage for AssetMetadataPipeline
TODO: need more coverage for GeoTiffPipeline
TODO: need coverage for MapMetadataToSTACItem
TODO: need coverage for MapGeoTiffToSTACItem
TODO: need coverage for STACCollectionBuilder
TODO: need coverage for GroupMetadataByYear and GroupMetadataByAttribute

Best to add unit tests in a bottom-up way.

"""

import datetime as dt
import pprint
from pathlib import Path
from typing import List

import pytest
from pystac.collection import Collection

from stacbuilder.boundingbox import BoundingBox
from stacbuilder.builder import (
    AlternateHrefGenerator,
    AssetMetadataPipeline,
)
from stacbuilder.collector import MetadataCollector
from stacbuilder.config import (
    AlternateHrefConfig,
    CollectionConfig,
    InputPathParserConfig,
)
from stacbuilder.exceptions import InvalidConfiguration
from stacbuilder.metadata import AssetMetadata, BandMetadata
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

    path_parser = MockPathParser()
    asset_path_data = path_parser.parse(asset_path)

    prefix_length = len("observations_")
    end_length = len("_2000-01-01.tif")
    asset_type = asset_path.name[prefix_length:-end_length]

    bbox_dict = {"east": 240.0, "epsg": 4326, "north": 0.0, "south": 180.0, "west": 0.0}

    md = AssetMetadata(
        asset_id=asset_path.stem,
        asset_type=asset_type,
        item_id=asset_path_data["item_id"],
        asset_path=asset_path,
        href=str(asset_path),
        original_href=str(asset_path),
        collection_id=asset_path_data["collection_id"],
        datetime=asset_path_data["datetime"],
        start_datetime=asset_path_data["start_datetime"],
        end_datetime=asset_path_data["end_datetime"],
        shape=(180, 240),
        tags={"AREA_OR_POINT": "Area"},
        file_size=asset_path.stat().st_size,
        bbox_projected=BoundingBox.from_dict(bbox_dict),
        transform=[1.0, 0.0, 0.0, 0.0, 1.0, 0.0],
        bands=[BandMetadata(data_type="float64", index=0, nodata=None)],
    )
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
    @pytest.fixture(scope="class")
    def geotiff_asset_metadata_pipeline(
        self, collection_config_from_file, file_collector_config, collection_output_dir
    ) -> AssetMetadataPipeline:
        return AssetMetadataPipeline.from_config(
            metadata_collector=MetadataCollector.from_config(
                collection_config=collection_config_from_file,
                file_coll_cfg=file_collector_config,
            ),
            collection_config=collection_config_from_file,
            output_dir=collection_output_dir,
            overwrite=False,
        )

    def test_get_input_files(self, geotiff_asset_metadata_pipeline: AssetMetadataPipeline, geotiff_paths: List[Path]):
        input_files = list(geotiff_asset_metadata_pipeline.get_input_files())

        assert sorted(input_files) == sorted(geotiff_paths)

    @pytest.mark.skip(reason="test files incorrect")
    def test_get_metadata(
        self, geotiff_asset_metadata_pipeline: AssetMetadataPipeline, basic_asset_metadata_list: List[Path]
    ):
        metadata_list = list(geotiff_asset_metadata_pipeline.get_metadata())

        sorted_actual_metadata_list = sorted(metadata_list)
        sorted_expected_metadata_list = sorted(basic_asset_metadata_list)

        expected_dicts = [e.to_dict() for e in sorted_expected_metadata_list]
        actual_dicts = [a.to_dict() for a in sorted_actual_metadata_list]

        for i, expected_md in enumerate(sorted_expected_metadata_list):
            pprint.pprint(expected_md.get_differences(sorted_actual_metadata_list[i]))

        assert expected_dicts == actual_dicts
        assert sorted_actual_metadata_list == sorted_expected_metadata_list

    def test_build_collection(self, geotiff_asset_metadata_pipeline: AssetMetadataPipeline):
        assert geotiff_asset_metadata_pipeline.collection is None

        geotiff_asset_metadata_pipeline.build_collection()

        assert geotiff_asset_metadata_pipeline.collection is not None
        assert geotiff_asset_metadata_pipeline.collection_file is not None
        assert geotiff_asset_metadata_pipeline.collection_file.exists()
        Collection.validate_all(geotiff_asset_metadata_pipeline.collection)

        collection = Collection.from_file(geotiff_asset_metadata_pipeline.collection_file)
        collection.validate_all()

    def test_build_grouped_collection(self, geotiff_asset_metadata_pipeline: AssetMetadataPipeline):
        geotiff_asset_metadata_pipeline.reset()
        assert geotiff_asset_metadata_pipeline.collection is None

        geotiff_asset_metadata_pipeline.build_grouped_collections()

        assert geotiff_asset_metadata_pipeline.collection_groups is not None
        assert geotiff_asset_metadata_pipeline.collection is None

        # Verify that each collection is written to its own separate file. (i.e. all paths are unique)
        collection_files = set(coll.self_href for coll in geotiff_asset_metadata_pipeline.collection_groups.values())
        assert len(collection_files) == len(geotiff_asset_metadata_pipeline.collection_groups)

        for coll in geotiff_asset_metadata_pipeline.collection_groups.values():
            # Each collection file must effectively exist.
            coll_path = Path(coll.self_href)
            coll_path.exists()

            # Each collection must be valid.
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
        asset_md = AssetMetadata(
            asset_id="asset123",
            item_id="item456",
            collection_id="collection789",
            asset_path=Path("/data/collection789/item456/asset123.tif"),
            datetime=dt.datetime(2023, 10, 1, 12, 0, 0, tzinfo=dt.UTC),
            bbox_projected=BoundingBox(4.0, 51.0, 5.0, 52.0, 4326),
        )
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

        assert alternate_generator.has_alternate_key("local") is False
        alternate_generator.add_MEP()
        assert alternate_generator.has_alternate_key("local") is True

        alternates = alternate_generator.get_alternates(simple_asset_metadata)

        assert alternates == {"alternate": {"local": {"href": "/data/collection789/item456/asset123.tif"}}}

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

        assert alternate_generator.has_alternate_key("local") is False
        assert alternate_generator.has_alternate_key("S3") is False

        alternate_generator.add_MEP()
        alternate_generator.add_basic_S3("test-bucket")
        assert alternate_generator.has_alternate_key("local") is True
        assert alternate_generator.has_alternate_key("S3") is True

        alternates = alternate_generator.get_alternates(simple_asset_metadata)
        assert alternates == {
            "alternate": {
                "local": {"href": "/data/collection789/item456/asset123.tif"},
                "S3": {"href": "s3://test-bucket/data/collection789/item456/asset123.tif"},
            }
        }

    def test_MEP_and_S3_with_root_path(self, simple_asset_metadata):
        alternate_generator = AlternateHrefGenerator()

        assert alternate_generator.has_alternate_key("local") is False
        assert alternate_generator.has_alternate_key("S3") is False

        alternate_generator.add_MEP()
        alternate_generator.add_basic_S3(s3_bucket="test-bucket", s3_root_path="test/data-root/path")

        assert alternate_generator.has_alternate_key("local") is True
        assert alternate_generator.has_alternate_key("S3") is True

        alternates = alternate_generator.get_alternates(simple_asset_metadata)
        assert alternates == {
            "alternate": {
                "local": {"href": "/data/collection789/item456/asset123.tif"},
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
            assert alt_href_gen.has_alternate_key("local") is False
            assert alt_href_gen.has_alternate_key("S3") is False
        else:
            assert alt_href_gen.has_alternate_key("local") == config.add_MEP
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
