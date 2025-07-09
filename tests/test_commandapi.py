"""Tests for the stacbuilder.builder module"""

from pathlib import Path

import pytest

from stacbuilder.commandapi import (
    build_collection,
    build_grouped_collections,
    list_asset_metadata,
    list_input_files,
    list_stac_items,
    load_collection,
    validate_collection,
)


class TestCommandAPI:
    @pytest.fixture
    def collection_config_file(self, data_dir) -> Path:
        return data_dir / "config/config-test-collection.json"

    def test_command_build_collection(self, data_dir, tmp_path):
        config_file = data_dir / "config/config-test-collection.json"
        input_dir = data_dir / "geotiff/mock-geotiffs"
        output_dir = tmp_path / "out-mock-geotiffs"

        build_collection(
            collection_config_path=config_file,
            glob="*/*.tif",
            input_dir=input_dir,
            output_dir=output_dir,
            overwrite=True,
        )
        # TODO: how to verify the output? For now this is just a smoke test.
        #   The underlying functionality can actually be tested more directly.

    def test_command_build_grouped_collections(self, data_dir, tmp_path):
        config_file = data_dir / "config/config-test-collection.json"
        input_dir = data_dir / "geotiff/mock-geotiffs"
        output_dir = tmp_path / "out-mock-geotiffs"

        build_grouped_collections(
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

        build_collection(
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
        list_input_files(collection_config_path=config_file, glob="*/*.tif", input_dir=input_dir)
        # TODO: how to verify the output? For now this is just a smoke test.
        #   The underlying functionality can actually be tested more directly.

    def test_command_list_asset_metadata(self, data_dir):
        config_file = data_dir / "config/config-test-collection.json"
        input_dir = data_dir / "geotiff/mock-geotiffs"
        list_asset_metadata(collection_config_path=config_file, glob="*/*.tif", input_dir=input_dir)
        # TODO: how to verify the output? For now this is just a smoke test.
        #   The underlying functionality can actually be tested more directly.

    def test_command_list_items(self, data_dir):
        config_file = data_dir / "config/config-test-collection.json"
        input_dir = data_dir / "geotiff/mock-geotiffs"
        list_stac_items(collection_config_path=config_file, glob="*/*.tif", input_dir=input_dir)
        # TODO: how to verify the output? For now this is just a smoke test.
        #   The underlying functionality can actually be tested more directly.

    def test_command_load_collection(self, data_dir, tmp_path):
        config_file = data_dir / "config/config-test-collection.json"
        input_dir = data_dir / "geotiff/mock-geotiffs"
        output_dir = tmp_path / "out-mock-geotiffs"

        build_collection(
            collection_config_path=config_file,
            glob="*/*.tif",
            input_dir=input_dir,
            output_dir=output_dir,
            overwrite=True,
        )
        collection_file = output_dir / "collection.json"
        load_collection(collection_file=collection_file)
        # TODO: how to verify the output? For now this is just a smoke test.
        #   The underlying functionality can actually be tested more directly.

    def test_command_validate_collection(self, data_dir, tmp_path):
        config_file = data_dir / "config/config-test-collection.json"
        input_dir = data_dir / "geotiff/mock-geotiffs"
        output_dir = tmp_path / "out-mock-geotiffs"

        build_collection(
            collection_config_path=config_file,
            glob="*/*.tif",
            input_dir=input_dir,
            output_dir=output_dir,
            overwrite=True,
        )
        collection_file = output_dir / "collection.json"
        validate_collection(collection_file=collection_file)
        # TODO: how to verify the output? For now this is just a smoke test.
        #   The underlying functionality can actually be tested more directly.
