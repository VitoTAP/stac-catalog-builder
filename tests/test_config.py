import pytest
from pydantic import ValidationError
from pystac.provider import ProviderRole

from stacbuilder.config import AssetConfig, BandConfig, CollectionConfig, ProviderModel


@pytest.fixture
def provider_acme() -> ProviderModel:
    return ProviderModel(
        name="ACME org",
        url="https://www.acme-faux-organisation.foo",
        roles=[ProviderRole.PRODUCER, ProviderRole.LICENSOR],
    )


@pytest.fixture
def collection_config() -> CollectionConfig:
    return CollectionConfig(
        collection_id="foo-2023-v01",
        title="The test collection",
        description="Description of the test collection",
        keywords=["keyword1", "keyword2"],
        mission=["foo-mission"],
        platform=["bar-platform"],
    )


def test_can_parse_providermodel_from_json():
    data = {"name": "Some EO org", "url": "https://www.some.place.in.space.dev/", "roles": ["producer", "processor"]}
    model = ProviderModel(**data)

    assert model.name == "Some EO org"
    assert model.url.unicode_string() == "https://www.some.place.in.space.dev/"
    assert model.roles == [ProviderRole.PRODUCER, ProviderRole.PROCESSOR]


class TestCollectionConfigModel:
    @pytest.fixture
    def data_dict(self):
        return {
            "collection_id": "foo-2023-v01",
            "title": "Foo collection",
            "description": "Description of Foo",
            "instruments": [],
            "keywords": ["foo", "bar", "oof"],
            "mission": [],
            "platform": [],
            "providers": [
                {
                    "name": "ACME-EO Company",
                    "roles": ["licensor", "processor", "producer"],
                    "url": "https://www.acme-eo.nowwhere.to.be.found.xyz/",
                }
            ],
        }

    def test_it_can_parse_dict(self):
        provider_data = {
            "name": "Some EO org",
            "url": "https://www.some.place.in.space.dev/",
            "roles": ["producer", "processor"],
        }
        provider_model = ProviderModel(**provider_data)

        data = {
            "collection_id": "foo-2023-v01",
            "title": "Foo collection",
            "description": "Description of Foo",
            "instruments": [],
            "keywords": ["foo", "bar", "oof"],
            "mission": [],
            "platform": [],
            "providers": [provider_data],
        }
        model = CollectionConfig(**data)

        assert model == CollectionConfig(
            collection_id="foo-2023-v01",
            title="Foo collection",
            description="Description of Foo",
            instruments=[],
            keywords=["foo", "bar", "oof"],
            mission=[],
            platform=[],
            providers=[provider_model],
        )


def test_asset_config_serializes_common_bands_from_bands_field():
    asset_cfg = AssetConfig(
        title="Band title",
        description="Band description",
        bands=[
            BandConfig(name="B01", description="Blue", data_type="uint16", unit="1"),
        ],
    )

    asset_def = asset_cfg.to_asset_definition()
    assert "bands" in asset_def.properties
    assert asset_def.properties["bands"] == [
        {
            "name": "B01",
            "description": "Blue",
            "data_type": "uint16",
            "unit": "1",
        }
    ]


def test_asset_config_merges_legacy_eo_and_raster_bands_into_common_bands():
    asset_cfg = AssetConfig(
        title="Band title",
        description="Band description",
        eo_bands=[{"name": "B01", "description": "Blue", "common_name": "blue"}],
        raster_bands=[{"name": "B01", "data_type": "uint16", "unit": "1"}],
    )

    common_bands = asset_cfg.get_common_bands()

    assert common_bands == [
        {
            "name": "B01",
            "description": "Blue",
            "eo:common_name": "blue",
            "data_type": "uint16",
            "unit": "1",
        }
    ]


def test_asset_config_keeps_prefixed_raster_fields_and_flags_extension_use():
    asset_cfg = AssetConfig(
        title="Band title",
        description="Band description",
        bands=[{"name": "B01", "raster:spatial_resolution": 10}],
    )

    common_bands = asset_cfg.get_common_bands()
    assert common_bands == [{"name": "B01", "raster:spatial_resolution": 10}]
    assert asset_cfg.uses_raster_extension() is True


def test_asset_config_keeps_prefixed_eo_fields_and_flags_extension_use():
    asset_cfg = AssetConfig(
        title="Band title",
        description="Band description",
        bands=[{"name": "B01", "eo:common_name": "red"}],
    )

    common_bands = asset_cfg.get_common_bands()
    assert common_bands == [{"name": "B01", "eo:common_name": "red"}]
    assert asset_cfg.uses_eo_extension() is True


def test_band_common_name_and_description_must_not_be_prefixed():
    with pytest.raises(ValidationError, match="must be unprefixed"):
        AssetConfig(
            title="Band title",
            description="Band description",
            bands=[{"eo:name": "B01", "eo:description": "Blue"}],
        )


@pytest.mark.parametrize(
    "data_type",
    [
        "int8",
        "int16",
        "int32",
        "int64",
        "uint8",
        "uint16",
        "uint32",
        "uint64",
        "float16",
        "float32",
        "float64",
        "cint16",
        "cint32",
        "cfloat32",
        "cfloat64",
        "other",
        "UINT16",
    ],
)
def test_band_data_type_allows_expected_values(data_type: str):
    band = BandConfig(name="B01", data_type=data_type)
    assert band.data_type in {
        "int8",
        "int16",
        "int32",
        "int64",
        "uint8",
        "uint16",
        "uint32",
        "uint64",
        "float16",
        "float32",
        "float64",
        "cint16",
        "cint32",
        "cfloat32",
        "cfloat64",
        "other",
    }


def test_band_data_type_unknown_values_fallback_to_other_and_warn():
    with pytest.warns(UserWarning, match="Invalid data_type 'bool'.*Falling back to 'other'"):
        band = BandConfig(name="B01", data_type="bool")
    assert band.data_type == "other"
