"""Tests for the stacbuilder.builder module
"""

import json
from pathlib import Path

import pytest

from stacbuilder.config import CollectionConfig

from stacbuilder.commandapi import (
    build_collection,
    build_grouped_collections,
    list_asset_metadata,
    list_input_files,
    list_stac_items,
    load_collection,
    postprocess_collection,
    validate_collection,
)


class TestCommandAPI:
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
            save_dataframe=True,
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
            save_dataframe=True,
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

    def test_command_postprocess_collection(
        self, data_dir, tmp_path, collection_config_file, collection_config_file_no_overrides
    ):
        input_dir = data_dir / "geotiff/mock-geotiffs"
        output_dir = tmp_path / "out-mock-geotiffs"

        build_collection(
            collection_config_path=collection_config_file_no_overrides,
            glob="*/*.tif",
            input_dir=input_dir,
            output_dir=output_dir,
            overwrite=True,
        )
        collection_file = output_dir / "collection.json"
        post_proc_dir = tmp_path / "post-processed"
        post_processed_coll_path = post_proc_dir / "collection.json"

        postprocess_collection(
            collection_file=collection_file, collection_config_path=collection_config_file, output_dir=post_proc_dir
        )

        # Check that the overrides were applied
        collection_as_dict = None
        with open(post_processed_coll_path, "r", encoding="utf8") as f:
            collection_as_dict = json.load(f)

        assert "level_1" in collection_as_dict
        assert "level_2" in collection_as_dict["level_1"]
        assert collection_as_dict["level_1"]["level_2"] == {"test_key": "test_value"}

    def test_command_postprocess_collection_when_noop(self, data_dir, tmp_path, collection_config_file_no_overrides):
        input_dir = data_dir / "geotiff/mock-geotiffs"
        output_dir = tmp_path / "out-mock-geotiffs"
        build_collection(
            collection_config_path=collection_config_file_no_overrides,
            glob="*/*.tif",
            input_dir=input_dir,
            output_dir=output_dir,
            overwrite=True,
        )
        collection_file: Path = output_dir / "collection.json"
        post_proc_dir = tmp_path / "post-processed"
        post_processed_coll_path: Path = post_proc_dir / "collection.json"

        postprocess_collection(
            collection_file=collection_file,
            collection_config_path=collection_config_file_no_overrides,
            output_dir=post_proc_dir,
        )

        original_contents = json.loads(collection_file.read_text(encoding="utf8"))
        processed_contents = json.loads(post_processed_coll_path.read_text(encoding="utf8"))
        assert original_contents == processed_contents
