"""Tests for the stacbuilder.builder module"""

import json
from pathlib import Path

import pytest
from nested_lookup import nested_update

from stacbuilder.commandapi import (
    build_collection,
    build_grouped_collections,
    list_asset_metadata,
    list_input_files,
    list_stac_items,
    load_collection,
    validate_collection,
)
from stacbuilder.stacapi.upload import Uploader


def compare_json_outputs(output_dir: Path, reference_dir: Path):
    """Compare JSON files in output_dir with those in reference_dir."""
    for file in output_dir.glob("**/*.json"):
        output_json = json.loads(file.read_text())
        output_json = nested_update(output_json, "created", "")

        def update_href(obj):
            if isinstance(obj, dict):
                for key, value in obj.items():
                    if key == "href" and isinstance(value, str) and not value.startswith("http"):
                        if Path(value).is_absolute():
                            new_href = Path("/stac-catalog-builder/tests") / Path(value).relative_to(
                                Path(__file__).parent
                            )
                            obj[key] = new_href.as_posix()
                    else:
                        update_href(value)
            elif isinstance(obj, list):
                for item in obj:
                    update_href(item)

        update_href(output_json)

        if "links" in output_json and isinstance(output_json["links"], list):
            output_json["links"].sort(key=lambda x: (x.get("rel", ""), x.get("href", "")))

        reference_path = reference_dir / file.relative_to(output_dir)
        if not reference_path.exists():
            reference_path.parent.mkdir(parents=True, exist_ok=True)
            with reference_path.open("w") as ref_file:
                json.dump(output_json, ref_file, indent=2)

        with reference_path.open("r") as ref_file:
            reference_json = json.load(ref_file)
        assert output_json == reference_json, "files do not match, run with -vv to see differences."
    assert len(list(output_dir.glob("**/*.json"))) == len(list(reference_dir.glob("**/*.json"))), (
        "Number of JSON files in output directory does not match reference directory."
    )


class TestCommandAPI:
    @pytest.fixture
    def collection_config_file(self, data_dir) -> Path:
        return data_dir / "config/config-test-collection.json"

    def test_command_build_collection(self, data_dir: Path, tmp_path: Path):
        config_file = data_dir / "config/config-test-collection.json"
        input_dir = data_dir / "geotiff/mock-geotiffs"
        output_dir = tmp_path / "out-mock-geotiffs"
        reference_dir = data_dir / "reference/basic"

        build_collection(
            collection_config_path=config_file,
            glob="*/*.tif",
            input_dir=input_dir,
            output_dir=output_dir,
        )

        # verify the output jsons
        compare_json_outputs(output_dir, reference_dir)

    def test_command_build_grouped_collections(self, data_dir, tmp_path):
        config_file = data_dir / "config/config-test-collection.json"
        input_dir = data_dir / "geotiff/mock-geotiffs"
        output_dir = tmp_path / "out-mock-geotiffs"
        reference_dir = data_dir / "reference/grouped"

        build_grouped_collections(
            collection_config_path=config_file,
            glob="*/*.tif",
            input_dir=input_dir,
            output_dir=output_dir,
        )

        # verify the output jsons
        compare_json_outputs(output_dir, reference_dir)

    def test_command_build_collection_unlinked(self, data_dir: Path, tmp_path: Path):
        config_file = data_dir / "config/config-test-collection.json"
        input_dir = data_dir / "geotiff/mock-geotiffs"
        output_dir = tmp_path / "out-mock-geotiffs"
        reference_dir = data_dir / "reference/unlinked"

        build_collection(
            collection_config_path=config_file,
            glob="*/*.tif",
            input_dir=input_dir,
            output_dir=output_dir,
            link_items=False,
        )

        # verify the output jsons
        compare_json_outputs(output_dir, reference_dir)

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
        )
        collection_file = output_dir / "collection.json"
        return collection_file

    def test_command_list_input_files(self, data_dir):
        input_dir = data_dir / "geotiff/mock-geotiffs"
        list_input_files(glob="*/*.tif", input_dir=input_dir)

    def test_command_list_asset_metadata(self, data_dir):
        config_file = data_dir / "config/config-test-collection.json"
        input_dir = data_dir / "geotiff/mock-geotiffs"
        returned_list = list_asset_metadata(collection_config_path=config_file, glob="*/*.tif", input_dir=input_dir)
        expected_count = 12
        assert len(returned_list) == expected_count, (
            f"Expected {expected_count} asset metadata items, got {len(returned_list)}."
        )

    def test_command_list_asset_metadata_with_glob(self, data_dir):
        config_file = data_dir / "config/config-test-collection.json"
        input_dir = data_dir / "geotiff/mock-geotiffs"
        returned_list = list_asset_metadata(collection_config_path=config_file, glob="2000/*.tif", input_dir=input_dir)
        expected_count = 6
        assert len(returned_list) == expected_count, (
            f"Expected {expected_count} asset metadata items, got {len(returned_list)}."
        )

    def test_command_list_items(self, data_dir):
        config_file = data_dir / "config/config-test-collection.json"
        input_dir = data_dir / "geotiff/mock-geotiffs"
        returned_items, failed_files = list_stac_items(
            collection_config_path=config_file, glob="*/*.tif", input_dir=input_dir
        )
        expected_count = 6
        assert len(returned_items) == expected_count, f"Expected {expected_count} items, got {len(returned_items)}."
        assert len(failed_files) == 0, f"Expected no failed files, got {len(failed_files)}."

    def test_command_load_collection(self, data_dir, tmp_path):
        config_file = data_dir / "config/config-test-collection.json"
        input_dir = data_dir / "geotiff/mock-geotiffs"
        output_dir = tmp_path / "out-mock-geotiffs"

        build_collection(
            collection_config_path=config_file,
            glob="*/*.tif",
            input_dir=input_dir,
            output_dir=output_dir,
        )
        collection_file = output_dir / "collection.json"
        returned_collection = load_collection(collection_file=collection_file)
        assert returned_collection is not None, "Expected a valid collection to be returned."
        assert returned_collection.id == "foo-2023-v01", (
            f"Expected collection ID 'test-collection', got '{returned_collection.id}'."
        )

    def test_command_validate_collection(self, data_dir, tmp_path):
        config_file = data_dir / "config/config-test-collection.json"
        input_dir = data_dir / "geotiff/mock-geotiffs"
        output_dir = tmp_path / "out-mock-geotiffs"

        build_collection(
            collection_config_path=config_file,
            glob="*/*.tif",
            input_dir=input_dir,
            output_dir=output_dir,
        )
        collection_file = output_dir / "collection.json"
        validate_collection(collection_file=collection_file)

    def test_command_build_collection_with_uploader(self, data_dir, requests_mock, mocker):
        """build_collection with an uploader should create the collection on the STAC API
        and upload all generated items in bulk batches, without writing anything to disk."""
        config_file = data_dir / "config/config-test-collection.json"
        input_dir = data_dir / "geotiff/mock-geotiffs"
        collection_id = "foo-2023-v01"
        stac_api_url = "http://test.stacapi.local"

        # Bypass STAC schema validation (requires network access)
        mocker.patch("pystac.Collection.validate", return_value=[])

        # Mock collection existence check → 404 (does not exist yet)
        requests_mock.get(f"{stac_api_url}/collections/{collection_id}", status_code=404)
        # Mock collection create → 201
        requests_mock.post(
            f"{stac_api_url}/collections",
            json={"id": collection_id},
            status_code=201,
        )
        # Mock bulk item upload → 200
        bulk_mock = requests_mock.post(
            f"{stac_api_url}/collections/{collection_id}/bulk_items",
            json={"message": "ok"},
            status_code=200,
        )

        bulk_size = 5
        uploader = Uploader.create_uploader(stac_api_url=stac_api_url, auth=None, bulk_size=bulk_size)

        build_collection(
            collection_config_path=config_file,
            glob="*/*.tif",
            input_dir=input_dir,
            uploader=uploader,
        )

        # The test data produces 6 STAC items: 12 input files (2 asset types × 3 months
        # × 2 years), grouped by item_id (one per unique date), giving 6 items each with
        # 2 assets.
        # With bulk_size=5, items are uploaded in 2 batches: first 5, then 1.
        expected_items = 6
        expected_bulk_calls = (expected_items + bulk_size - 1) // bulk_size  # ceil division
        assert bulk_mock.call_count == expected_bulk_calls, (
            f"Expected {expected_bulk_calls} bulk upload call(s) for {expected_items} items "
            f"with bulk_size={bulk_size}, got {bulk_mock.call_count}"
        )

    def test_command_build_collection_with_uploader_merges_extents(self, data_dir, requests_mock, mocker):
        """When the collection already exists on the API, build_collection with uploader
        should merge extents and call PUT (update) rather than POST (create)."""
        import datetime as dt

        from pystac import Collection, Extent, SpatialExtent, TemporalExtent

        config_file = data_dir / "config/config-test-collection.json"
        input_dir = data_dir / "geotiff/mock-geotiffs"
        collection_id = "foo-2023-v01"
        stac_api_url = "http://test.stacapi.local"

        # Bypass STAC schema validation
        mocker.patch("pystac.Collection.validate", return_value=[])

        # Existing collection on API with a narrow extent
        existing = Collection(
            id=collection_id,
            description="existing",
            extent=Extent(
                SpatialExtent([[-5.0, -5.0, 5.0, 5.0]]),
                TemporalExtent([[dt.datetime(2019, 1, 1), dt.datetime(2019, 12, 31)]]),
            ),
        )
        requests_mock.get(
            f"{stac_api_url}/collections/{collection_id}", json=existing.to_dict(), status_code=200
        )
        put_mock = requests_mock.put(
            f"{stac_api_url}/collections/{collection_id}",
            json={"id": collection_id},
            status_code=200,
        )
        post_mock = requests_mock.post(f"{stac_api_url}/collections", status_code=201)
        requests_mock.post(
            f"{stac_api_url}/collections/{collection_id}/bulk_items",
            json={"message": "ok"},
            status_code=200,
        )

        uploader = Uploader.create_uploader(stac_api_url=stac_api_url, auth=None, bulk_size=5)

        build_collection(
            collection_config_path=config_file,
            glob="*/*.tif",
            input_dir=input_dir,
            uploader=uploader,
        )

        # Collection should have been updated (PUT), not created (POST)
        assert put_mock.called
        assert not post_mock.called
