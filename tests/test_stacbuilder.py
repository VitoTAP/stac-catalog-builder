from pathlib import Path

import pytest

from stacbuilder.builder import STACBuilder
from stacbuilder.builder import command_build_collection, command_gather_inputs

from stacbuilder.config import CollectionConfig


@pytest.fixture
def stac_builder(data_dir) -> STACBuilder:
    builder = STACBuilder()
    # builder.input_dir = data_dir
    # builder.glob = "*"
    builder.input_dir = data_dir / "geotiff/reanalysis-era5-land-mm"
    builder.glob = "*/*"
    builder.output_dir = Path(".local/test-out")
    builder.overwrite = True

    return builder

@pytest.fixture
def stac_builder_anin(data_dir) -> STACBuilder:
    builder = STACBuilder()
    builder.input_dir = data_dir / "geotiff/reanalysis-era5-land-mm"
    builder.glob = "*/*"
    builder.output_dir = Path(".local/test-out")
    builder.overwrite = True

    return builder


@pytest.fixture
def collection_config() -> CollectionConfig:

    provider_data = {
        "name": "Some EO org",
        "url": "https://www.some.place.in.space.dev/",
        "roles": ["producer", "processor"]
    }

    data = {
        'collection_id': "foo-2023-v01",
        'title': 'Foo collection',
        'description': 'Description of Foo',
        'instruments': [],
        'keywords': ['foo', 'bar', 'oof'],
        'mission': [],
        'platform': [],
        'providers': [provider_data],
    }
    return CollectionConfig(**data)

    # try:
    #     config = CollectionConfig(
    #         title="The test collection",
    #         description="Description of the test collection",
    #         keywords=["keyword1", "keyword2"],
    #         mission=["foo-mission"],
    #         platform=["bar-platform"]
    #     )
    # except Exception as exc:
    #     import traceback
    #     # traceback.print_exception(exc)
    #     traceback.print_exc
    #     raise
    # else:
    #     return config

@pytest.fixture
def collection_config_anin() -> CollectionConfig:

    provider_data = {
        "name": "cds.climate.copernicus.eu",
        "roles": [
            "licensor",
            "processor",
            "producer"
        ],
        "url": "https://cds.climate.copernicus.eu/"
    }
    data = {
        "collection_id": "reanalysis-era5-land-monthly-means_world",
        "title": "Reanalysis ERA5-land monthly means entire world",
        "description": "Reanalysis ERA5-land monthly means, entire world",
        "instruments": [],
        "keywords": ["ERA5", "CDS", "Copernicus", "climate"],
        "mission": [],
        "platform": [],
        "providers": [provider_data],
        "input_path_parser": "ANINPathParser"
    }
    model = CollectionConfig(**data)
    return model


class TestSTACBuilder:

    def test_pre_run_check(
            self,
            stac_builder: STACBuilder,
            collection_config_anin: CollectionConfig
        ):
        stac_builder.collection_config = collection_config_anin
        stac_builder.validate_builder_settings()

    def test_gather_input_files(
            self, 
            stac_builder: STACBuilder,
            collection_config_anin: CollectionConfig
        ):
        stac_builder.collection_config = collection_config_anin
        stac_builder.collect_input_files()

        input_dir = stac_builder.input_dir

        expected_files_relative = [
            "1980/reanalysis-era5-land-monthly-means_2m_temperature_monthly_19800101.tif",
            "1980/reanalysis-era5-land-monthly-means_2m_temperature_monthly_19800201.tif",
            "1980/reanalysis-era5-land-monthly-means_total_precipitation_monthly_19800101.tif",
            "1980/reanalysis-era5-land-monthly-means_total_precipitation_monthly_19800201.tif",
            "1981/reanalysis-era5-land-monthly-means_2m_temperature_monthly_19810101.tif",
            "1981/reanalysis-era5-land-monthly-means_2m_temperature_monthly_19810201.tif",
            "1981/reanalysis-era5-land-monthly-means_total_precipitation_monthly_19810101.tif",
            "1981/reanalysis-era5-land-monthly-means_total_precipitation_monthly_19810201.tif",
        ]
        expected_files = sorted(input_dir / f for f in expected_files_relative)
        assert sorted(stac_builder.input_files) == expected_files

    def test_create_collection(
            self,
            stac_builder: STACBuilder,
            collection_config_anin: CollectionConfig
        ):
        stac_builder.collection_config = collection_config_anin
        stac_builder.collect_input_files(max_number=4)
        
        assert stac_builder.collection_config == collection_config_anin

        stac_builder.create_collection()
        stac_builder.validate_collection()



class TestCommandAPI:

    def test_command_build_collection(self, data_dir, tmp_path):
        config_file = data_dir / "config/config-anin-collection.json"
        input_dir = data_dir / "geotiff/reanalysis-era5-land-mm"
        output_dir = tmp_path / "out-reanalysis-era5-land-mm"

        command_build_collection(
            collection_config_path=config_file,
            glob="*/*.tif",
            input_dir=input_dir,
            output_dir=output_dir,
            overwrite=False
        )

    def test_command_gather_inputs(self, data_dir):
        config_file = data_dir / "config/config-anin-collection.json"
        input_dir = data_dir / "geotiff/reanalysis-era5-land-mm"
        command_gather_inputs(
            collection_config_path=config_file,
            glob="*/*.tif",
            input_dir=input_dir
        )