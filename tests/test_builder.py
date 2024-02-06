"""Tests for the stacbuilder.builder module

Classes we really need to add coverage for:

TODO: need coverage for GeoTiffPipeline
TODO: need coverage for STACCollectionBuilder
TODO: need coverage for MapMetadataToSTACItem
TODO: need coverage for MapGeoTiffToSTACItem
TODO: need coverage for GroupMetadataByYear and GroupMetadataByAttribute
TODO: need coverage for PostProcessSTACCollectionFile

Best to add unit tests in a bottom-up way.

"""
from pathlib import Path
from typing import List


import pytest
import rasterio
import numpy as np
from pystac.collection import Collection


from stacbuilder.builder import (
    AlternateHrefGenerator,
    MEPAlternateLinksGenerator,
    S3AlternateLinksGenerator,
    GeoTiffPipeline,
)
from stacbuilder.config import CollectionConfig, FileCollectorConfig, InputPathParserConfig
from stacbuilder.metadata import AssetMetadata


@pytest.fixture
def collection_config_path(data_dir: Path) -> Path:
    return data_dir / "config" / "config-test-collection.json"


@pytest.fixture
def collection_config_from_file(collection_config_path: Path) -> CollectionConfig:
    return CollectionConfig.from_json_file(collection_config_path)


@pytest.fixture
def file_collector_config(geotiff_input_dir: Path) -> FileCollectorConfig:
    return FileCollectorConfig(input_dir=geotiff_input_dir, glob="*/*.tif")


@pytest.fixture
def collection_output_dir(tmp_path) -> Path:
    return tmp_path / "collection-no-groups"


@pytest.fixture
def grouped_collection_output_dir(tmp_path) -> Path:
    return tmp_path / "collections-per-group"


@pytest.fixture
def geotiff_pipeline(collection_config_from_file, file_collector_config, collection_output_dir) -> GeoTiffPipeline:
    return GeoTiffPipeline.from_config(
        collection_config=collection_config_from_file,
        file_coll_cfg=file_collector_config,
        output_dir=collection_output_dir,
        overwrite=False,
    )


@pytest.fixture
def geotiff_pipeline_grouped(
    grouped_collection_test_config, file_collector_config, grouped_collection_output_dir
) -> GeoTiffPipeline:
    return GeoTiffPipeline.from_config(
        collection_config=grouped_collection_test_config,
        file_coll_cfg=file_collector_config,
        output_dir=grouped_collection_output_dir,
        overwrite=False,
    )


@pytest.fixture
def geotif_paths_relative() -> List[str]:
    return sorted(
        [
            "2000/observations_2m-temp-monthly_2000-01-01.tif",
            "2000/observations_2m-temp-monthly_2000-02-01.tif",
            "2000/observations_2m-temp-monthly_2000-03-01.tif",
            "2000/observations_tot-precip-monthly_2000-01-01.tif",
            "2000/observations_tot-precip-monthly_2000-02-01.tif",
            "2000/observations_tot-precip-monthly_2000-03-01.tif",
            "2001/observations_2m-temp-monthly_2001-01-01.tif",
            "2001/observations_2m-temp-monthly_2001-02-01.tif",
            "2001/observations_2m-temp-monthly_2001-03-01.tif",
            "2001/observations_tot-precip-monthly_2001-01-01.tif",
            "2001/observations_tot-precip-monthly_2001-02-01.tif",
            "2001/observations_tot-precip-monthly_2001-03-01.tif",
        ]
    )


@pytest.fixture
def geotiff_paths(data_dir, geotif_paths_relative) -> List[Path]:
    input_dir = data_dir / "geotiff/mock-geotiffs"
    return generate_geotiff_paths(input_dir, geotif_paths_relative)


def generate_geotiff_paths(input_dir, geotif_paths_relative) -> List[Path]:
    return sorted(input_dir / f for f in geotif_paths_relative)


def create_geotiff_files(paths):
    for file in paths:
        if not file.parent.exists():
            file.parent.mkdir(parents=True)
        create_mock_geotiff(file)


def create_mock_geotiff(tif_path: Path):
    # Based on the example in rasterio docs:
    # https://rasterio.readthedocs.io/en/stable/quickstart.html#opening-a-dataset-in-writing-mode
    x = np.linspace(-4.0, 4.0, 240)
    y = np.linspace(-3.0, 3.0, 180)
    X, Y = np.meshgrid(x, y)
    Z1 = np.exp(-2 * np.log(2) * ((X - 0.5) ** 2 + (Y - 0.5) ** 2) / 1**2)
    Z2 = np.exp(-3 * np.log(2) * ((X + 0.5) ** 2 + (Y + 0.5) ** 2) / 2.5**2)
    Z = 10.0 * (Z2 - Z1)

    new_dataset = rasterio.open(
        tif_path,
        "w",
        driver="GTiff",
        height=Z.shape[0],
        width=Z.shape[1],
        count=1,
        dtype=Z.dtype,
        crs=4326,
        # transform=transform,
    )
    new_dataset.write(Z, 1)
    new_dataset.close()


@pytest.mark.skip
def test_create_tiffs(geotiff_paths):
    # TODO: [refactor]: This method should not be a test but a script to generate test data.
    #   Also, you only need to run that script one time. But now the test tiffs are
    #   stored in git.
    create_geotiff_files(geotiff_paths)


@pytest.fixture
def collection_test_config() -> CollectionConfig:
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
                "regex_pattern": r".*_(?P<asset_type>[a-zA-Z0-9\-]+)_(?P<datetime>\d{4}-\d{2}-\d{2})\.tif$",
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


@pytest.fixture
def grouped_collection_test_config() -> CollectionConfig:
    data = {
        "collection_id": "foo-2023-v01_grouped",
        "title": "Foo collection with groups per year",
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
                "regex_pattern": r".*_(?P<asset_type>[a-zA-Z0-9\-]+)_(?P<datetime>\d{4}-\d{2}-\d{2})\.tif$",
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


class TestGeoTiffPipeline:
    def test_collect_input_files(self, geotiff_pipeline: GeoTiffPipeline, geotiff_paths: List[Path]):
        input_files = list(geotiff_pipeline.get_input_files())

        assert sorted(input_files) == sorted(geotiff_paths)

    def test_build_collection(self, geotiff_pipeline: GeoTiffPipeline):
        assert geotiff_pipeline.collection is None

        geotiff_pipeline.build_collection()

        assert geotiff_pipeline.collection is not None
        assert geotiff_pipeline.collection_file is not None
        assert geotiff_pipeline.collection_file.exists()
        Collection.validate_all(geotiff_pipeline.collection)

        collection = Collection.from_file(geotiff_pipeline.collection_file)
        collection.validate_all()

    def test_build_grouped_collection(self, geotiff_pipeline_grouped: GeoTiffPipeline):
        assert geotiff_pipeline_grouped.collection is None

        geotiff_pipeline_grouped.build_grouped_collections()

        assert geotiff_pipeline_grouped.collection_groups is not None
        assert geotiff_pipeline_grouped.collection is None

        for coll in geotiff_pipeline_grouped.collection_groups.values():
            coll_path = Path(coll.self_href)
            coll_path.exists()

            collection = Collection.from_file(coll_path)
            collection.validate_all()


@pytest.fixture
def simple_asset_metadata() -> AssetMetadata:
    asset_md = AssetMetadata()
    asset_md.asset_id = "asset123"
    asset_md.item_id = "item456"
    asset_md.collection_id = "collection789"
    asset_md.asset_path = Path("/data/collection789/item456/asset123.tif")

    return asset_md


class TestAlternateLinksGenerator:
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
        alternate_generator.add_basic_S3("test-bucket")
        assert alternate_generator.has_alternate_key("S3") is True

        alternates = alternate_generator.get_alternates(simple_asset_metadata)
        assert alternates == {"alternate": {"S3": {"href": "s3://test-bucket/data/collection789/item456/asset123.tif"}}}

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


class TestMEPAlternateLinksGenerator:
    def test_get_alternates(self, simple_asset_metadata):
        alternate_generator = MEPAlternateLinksGenerator()
        alternates = alternate_generator.get_alternates(simple_asset_metadata)
        assert alternates == {"alternate": {"MEP": {"href": "/data/collection789/item456/asset123.tif"}}}


class TestS3AlternateLinksGenerator:
    def test_get_alternates_only_s3_bucket(self, simple_asset_metadata):
        alternate_generator = S3AlternateLinksGenerator(s3_bucket="test-bucket")
        alternates = alternate_generator.get_alternates(simple_asset_metadata)
        assert alternates == {"alternate": {"S3": {"href": "s3://test-bucket/data/collection789/item456/asset123.tif"}}}

    def test_get_alternates_with_root_path(self, simple_asset_metadata):
        alternate_generator = S3AlternateLinksGenerator(s3_bucket="test-bucket", s3_root_path="test/data-root/path")
        alternates = alternate_generator.get_alternates(simple_asset_metadata)
        assert alternates == {
            "alternate": {
                "S3": {"href": "s3://test-bucket/test/data-root/path/data/collection789/item456/asset123.tif"}
            }
        }
