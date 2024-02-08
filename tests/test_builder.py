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
import datetime as dt
from pathlib import Path
import pprint
from typing import List


import pytest
import rasterio
from rasterio.io import DatasetWriter
import numpy as np
from pystac.collection import Collection


from stacbuilder.boundingbox import BoundingBox
from stacbuilder.builder import (
    AlternateHrefGenerator,
    AssetMetadataPipeline,
    GeoTiffPipeline,
    GeoTiffMetadataCollector,
    IMetadataCollector,
    FileCollector,
)
from stacbuilder.config import (
    AlternateHrefConfig,
    CollectionConfig,
    FileCollectorConfig,
    InputPathParserConfig,
)
from stacbuilder.exceptions import InvalidConfiguration
from stacbuilder.metadata import AssetMetadata, RasterMetadata, BandMetadata
from stacbuilder.pathparsers import RegexInputPathParser


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
def geotif_paths_relative_small_set() -> List[str]:
    return sorted(
        [
            "2000/observations_2m-temp-monthly_2000-01-01.tif",
            "2000/observations_tot-precip-monthly_2000-01-01.tif",
        ]
    )


@pytest.fixture
def geotiff_paths(data_dir, geotif_paths_relative) -> List[Path]:
    input_dir = data_dir / "geotiff/mock-geotiffs"
    return generate_geotiff_paths(input_dir, geotif_paths_relative)


@pytest.fixture
def geotiff_paths_small_set(data_dir, geotif_paths_relative_small_set) -> List[Path]:
    input_dir = data_dir / "geotiff/mock-geotiffs"
    return generate_geotiff_paths(input_dir, geotif_paths_relative_small_set)


def generate_geotiff_paths(input_dir, geotif_paths_relative) -> List[Path]:
    return sorted(input_dir / f for f in geotif_paths_relative)


def create_geotiff_files(paths):
    for file in paths:
        if not file.parent.exists():
            file.parent.mkdir(parents=True)
        create_mock_geotiff(file)


# TODO: move create_mock_geotiff and test_create_tiffs to a non-test module that generates test data.
def create_mock_geotiff(tif_path: Path):
    """Create GeoTIFF raster files for testing.

    The pixels form a simple gradient just to have some data.
    """
    # Based on the example in rasterio docs:
    # https://rasterio.readthedocs.io/en/stable/quickstart.html#opening-a-dataset-in-writing-mode
    x = np.linspace(-4.0, 4.0, 240)
    y = np.linspace(-3.0, 3.0, 180)
    X, Y = np.meshgrid(x, y)
    Z1 = np.exp(-2 * np.log(2) * ((X - 0.5) ** 2 + (Y - 0.5) ** 2) / 1**2)
    Z2 = np.exp(-3 * np.log(2) * ((X + 0.5) ** 2 + (Y + 0.5) ** 2) / 2.5**2)
    Z = 10.0 * (Z2 - Z1)

    dataset: DatasetWriter = rasterio.open(
        tif_path,
        "w",
        driver="GTiff",
        height=Z.shape[0],
        width=Z.shape[1],
        count=1,
        dtype=Z.dtype,
        crs=4326,
    )
    dataset.write(Z, 1)
    dataset.close()


@pytest.mark.skip
def test_create_tiffs(geotiff_paths):
    # TODO: [refactor]: This method should not be a test but a script to generate test data.
    #   Also, you only need to run that script one time. But now the test tiffs are
    #   stored in git.
    create_geotiff_files(geotiff_paths)


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
            classname="MockPathParser",
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


class MockMetadataCollector(IMetadataCollector):
    """A mock implementation of IMetadataCollector.

    You give it some fixed AssetMetadata you want it to return.
    The `collect` method is a no-op in this case
    """

    def __init__(self, asset_metadata_list: List[AssetMetadata]):
        self._metadata_list = asset_metadata_list

    def add_asset(self, asset_md: AssetMetadata):
        if self._metadata_list is None:
            self._metadata_list = []
        self._metadata_list.append(asset_md)

    def collect(self) -> None:
        pass


class MockPathParser(RegexInputPathParser):
    def __init__(self, *args, **kwargs) -> None:
        type_converters = {
            "year": int,
            "month": int,
            "day": int,
        }
        fixed_values = {"collection_id": "observations"}
        regex_pattern = ".*observations_(?P<asset_type>.*)_(?P<year>\\d{4})-(?P<month>\\d{2})-(?P<day>\\d{2}).*\\.tif$"
        super().__init__(
            regex_pattern=regex_pattern, type_converters=type_converters, fixed_values=fixed_values, *args, **kwargs
        )

    def _post_process_data(self):
        start_dt = self._derive_start_datetime()
        self._data["datetime"] = start_dt
        self._data["start_datetime"] = start_dt
        self._data["end_datetime"] = self._derive_end_datetime()

        year = self._data["year"]
        self._data["item_id"] = f"observations_{self._data['asset_type']}_{year:04}"

    def _derive_start_datetime(self):
        """Derive the start datetime from other properties that were extracted."""
        year = self._data.get("year")
        month = self._data.get("month")
        day = self._data.get("day")

        if not (year and month and day):
            print(
                "WARNING: Could not find all date fields: "
                + f"{year=}, {month=}, {day=}, {self._data=},\n{self._path=}\n{self._regex.pattern=}"
            )
            return None

        return dt.datetime(year, month, day, 0, 0, 0, tzinfo=dt.timezone.utc)

    def _derive_end_datetime(self):
        """Derive the end datetime from other properties that were extracted."""
        start_dt = self._derive_start_datetime()
        if not start_dt:
            print(
                "WARNING: Could not determine start_datetime: " + f"{self._data=}, {self._path=}, {self._regex.pattern}"
            )
            return None

        year = start_dt.year
        return dt.datetime(year, 12, 31, 23, 59, 59, tzinfo=dt.timezone.utc)


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

        # Validation should not raise any expections of type pystac.errors.STACValidationError
        for item in stac_items:
            item.validate()

    def test_build_collection(self, asset_metadata_pipeline: AssetMetadataPipeline):
        assert asset_metadata_pipeline.collection is None

        asset_metadata_pipeline.build_collection()

        assert asset_metadata_pipeline.collection is not None
        assert asset_metadata_pipeline.collection_file is not None
        assert asset_metadata_pipeline.collection_file.exists()
        Collection.validate_all(asset_metadata_pipeline.collection)

        # Validation should not raise any expections of type pystac.errors.STACValidationError
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

            # Validation should not raise any expections of type pystac.errors.STACValidationError
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


class TestGeoTiffMetadataCollector:
    @pytest.fixture
    def geotiff_meta_collector(
        self,
        collection_config_from_file,
        file_collector_config,
    ) -> GeoTiffMetadataCollector:
        return GeoTiffMetadataCollector.from_config(
            collection_config=collection_config_from_file, file_coll_cfg=file_collector_config
        )

    def test_constructor(self, collection_config_from_file, file_collector_config):
        file_collector = FileCollector.from_config(file_collector_config)
        amd_collector = GeoTiffMetadataCollector(
            collection_config=collection_config_from_file, file_collector=file_collector
        )
        assert amd_collector.collection_config is collection_config_from_file
        assert amd_collector.file_collector is not None
        assert amd_collector.geotiff_to_metadata_mapper is not None

    def test_from_config(self, collection_config_from_file, file_collector_config):
        amd_collector = GeoTiffMetadataCollector.from_config(
            collection_config=collection_config_from_file, file_coll_cfg=file_collector_config
        )

        assert amd_collector.collection_config is collection_config_from_file
        assert amd_collector.file_collector is not None
        assert amd_collector.geotiff_to_metadata_mapper is not None

    def test_has_collected_is_false_before_collection(self, geotiff_meta_collector):
        assert geotiff_meta_collector.has_collected() is False

    def test_has_collected_is_true_after_collection(self, geotiff_meta_collector):
        geotiff_meta_collector.collect()
        assert geotiff_meta_collector.has_collected() is True

    def test_has_collected_finds_metadata(self, geotiff_meta_collector, geotiff_paths):
        geotiff_meta_collector.collect()
        assert len(geotiff_meta_collector.metadata_list) == 12

        actual_asset_paths = [am.asset_path for am in geotiff_meta_collector.metadata_list]
        assert sorted(actual_asset_paths) == sorted(geotiff_paths)
