from pathlib import Path
from typing import List


import pytest
import rasterio
import numpy as np
from pystac.collection import Collection


from stacbuilder.builder import (
    STACBuilder,
    CommandsNewPipeline,
    command_build_collection,
    command_load_collection,
    command_validate_collection,
    command_post_process_collection,
)
from stacbuilder.config import CollectionConfig, InputPathParserConfig, AssetConfig, EOBandConfig


@pytest.fixture
def stac_builder(data_dir, tmp_path) -> STACBuilder:
    builder = STACBuilder()
    builder.input_dir = data_dir / "geotiff/mock-geotiffs"
    builder.glob = "*/*"
    builder.output_dir = tmp_path / "stac-output"
    builder.overwrite = True

    return builder


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
                "regex_pattern": r".*_(?P<band>[a-zA-Z0-9\-]+)_(?P<datetime>\d{4}-\d{2}-\d{2})\.tif$",
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


class TestSTACBuilder:
    def test_validate_builder_settings(self, stac_builder: STACBuilder, collection_test_config: CollectionConfig):
        stac_builder.collection_config = collection_test_config
        stac_builder.validate_builder_settings()

    @pytest.mark.skip
    def test_create_tiffs(self, geotiff_paths):
        # TODO: [refactor]: This method should not be a test but a script to generate test data.
        #   Also, you only need to run that script one time. But now the test tiffs are
        #   stored in git.
        create_geotiff_files(geotiff_paths)

    def test_collect_input_files(
        self, stac_builder: STACBuilder, collection_test_config: CollectionConfig, geotiff_paths
    ):
        stac_builder.collection_config = collection_test_config
        stac_builder.collect_input_files()

        assert sorted(stac_builder.input_files) == sorted(geotiff_paths)

    def test_create_collection(self, stac_builder: STACBuilder, collection_test_config: CollectionConfig):
        stac_builder.collection_config = collection_test_config
        stac_builder.collect_input_files()
        assert stac_builder.collection_config == collection_test_config

        collection = stac_builder.create_collection()
        assert stac_builder.collection
        assert collection == stac_builder.collection

    def test_validate_collection(self, stac_builder: STACBuilder, collection_test_config: CollectionConfig):
        stac_builder.collection_config = collection_test_config
        stac_builder.collect_input_files()
        assert stac_builder.collection_config == collection_test_config

        collection = stac_builder.create_collection()
        assert stac_builder.collection
        assert collection == stac_builder.collection

        stac_builder.validate_collection(collection)

    def test_build_collection(self, stac_builder: STACBuilder, collection_test_config: CollectionConfig):
        stac_builder.collection_config = collection_test_config

        assert not stac_builder.collection_file.exists()

        stac_builder.build_collection()
        assert stac_builder.collection_file.exists()

        collection = Collection.from_file(stac_builder.collection_file)
        collection.validate_all()


class TestCommandAPI:
    def test_command_build_collection(self, data_dir, tmp_path):
        config_file = data_dir / "config/config-test-collection.json"
        input_dir = data_dir / "geotiff/mock-geotiffs"
        output_dir = tmp_path / "out-mock-geotiffs"

        command_build_collection(
            collection_config_path=config_file,
            glob="*/*.tif",
            input_dir=input_dir,
            output_dir=output_dir,
            overwrite=True,
        )
        # TODO: how to verify the output? For now this is just a smoke test.
        #   The underlying functionality can actually be tested more directly.

    @pytest.fixture
    def valid_collection_file(self, data_dir, tmp_path):
        config_file = data_dir / "config/config-test-collection.json"
        input_dir = data_dir / "geotiff/mock-geotiffs"
        output_dir = tmp_path / "out-mock-geotiffs"

        command_build_collection(
            collection_config_path=config_file,
            glob="*/*.tif",
            input_dir=input_dir,
            output_dir=output_dir,
            overwrite=True,
        )
        collection_file = output_dir / "collection.json"
        return collection_file

    def command_list_input_files(self, data_dir):
        config_file = data_dir / "config/config-test-collection.json"
        input_dir = data_dir / "geotiff/mock-geotiffs"
        CommandsNewPipeline.command_list_input_files(collection_config_path=config_file, glob="*/*.tif", input_dir=input_dir)
        # TODO: how to verify the output? For now this is just a smoke test.
        #   The underlying functionality can actually be tested more directly.

    def test_command_list_metadata(self, data_dir):
        config_file = data_dir / "config/config-test-collection.json"
        input_dir = data_dir / "geotiff/mock-geotiffs"
        CommandsNewPipeline.command_list_metadata(collection_config_path=config_file, glob="*/*.tif", input_dir=input_dir)
        # TODO: how to verify the output? For now this is just a smoke test.
        #   The underlying functionality can actually be tested more directly.

    def test_command_list_items(self, data_dir):
        config_file = data_dir / "config/config-test-collection.json"
        input_dir = data_dir / "geotiff/mock-geotiffs"
        CommandsNewPipeline.command_list_stac_items(collection_config_path=config_file, glob="*/*.tif", input_dir=input_dir)
        # TODO: how to verify the output? For now this is just a smoke test.
        #   The underlying functionality can actually be tested more directly.

    def test_command_load_collection(self, data_dir, tmp_path):
        config_file = data_dir / "config/config-test-collection.json"
        input_dir = data_dir / "geotiff/mock-geotiffs"
        output_dir = tmp_path / "out-mock-geotiffs"

        command_build_collection(
            collection_config_path=config_file,
            glob="*/*.tif",
            input_dir=input_dir,
            output_dir=output_dir,
            overwrite=True,
        )
        collection_file = output_dir / "collection.json"
        command_load_collection(collection_file=collection_file)
        # TODO: how to verify the output? For now this is just a smoke test.
        #   The underlying functionality can actually be tested more directly.

    def test_command_validate_collection(self, data_dir, tmp_path):
        config_file = data_dir / "config/config-test-collection.json"
        input_dir = data_dir / "geotiff/mock-geotiffs"
        output_dir = tmp_path / "out-mock-geotiffs"

        command_build_collection(
            collection_config_path=config_file,
            glob="*/*.tif",
            input_dir=input_dir,
            output_dir=output_dir,
            overwrite=True,
        )
        collection_file = output_dir / "collection.json"
        command_validate_collection(collection_file=collection_file)
        # TODO: how to verify the output? For now this is just a smoke test.
        #   The underlying functionality can actually be tested more directly.

    def test_command_post_process_collection(self, data_dir, tmp_path):
        config_file = data_dir / "config/config-test-collection.json"
        input_dir = data_dir / "geotiff/mock-geotiffs"
        output_dir = tmp_path / "out-mock-geotiffs"

        command_build_collection(
            collection_config_path=config_file,
            glob="*/*.tif",
            input_dir=input_dir,
            output_dir=output_dir,
            overwrite=True,
        )
        collection_file = output_dir / "collection.json"
        post_proc_dir = tmp_path / "post-processed"

        command_post_process_collection(
            collection_file=collection_file, collection_config_path=config_file, output_dir=post_proc_dir
        )
        # TODO: how to verify the output? For now this is just a smoke test.
        #   The underlying functionality can actually be tested more directly.
