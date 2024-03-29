import pytest

from stacbuilder.collector import FileCollector, GeoTiffMetadataCollector


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
