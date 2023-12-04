import pytest


from stacbuilder.config import CollectionConfigForm, CollectionConfig, ProviderModel

from pystac.provider import ProviderRole


from stacbuilder.config import InputsForm


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
    assert model.roles == {ProviderRole.PRODUCER, ProviderRole.PROCESSOR}


class TestCollectionForm:
    def test_fill_and_validate_collection_form(self):
        # provider = ProviderModel(name="VITO", url="https://www.vito.be")

        form = CollectionConfigForm()
        form.collection_id = "foo-2023-v01"
        form.title = "Foo is a Bar"
        form.description = "Description of Foo"
        form.keywords = ["foo", "bar", "oof"]
        form.providers = [ProviderModel(name="VITO", url="https://www.vito.be")]

        model = form.get_model()
        expected_config = CollectionConfig(
            collection_id="foo-2023-v01",
            title="Foo is a Bar",
            description="Description of Foo",
            keywords=["foo", "bar", "oof"],
            providers=[ProviderModel(name="VITO", url="https://www.vito.be")],
        )
        assert model == expected_config

    def test_is_valid_returns_true(self):
        # provider = ProviderModel(name="VITO", url="https://www.vito.be")

        form = CollectionConfigForm()
        form.collection_id = "foo-2023-v01"
        form.title = "Foo is a Bar"
        form.description = "Description of Foo"
        form.keywords = ["foo", "bar", "oof"]
        form.providers = [ProviderModel(name="VITO", url="https://www.vito.be")]

        assert form.is_valid is True

    @pytest.mark.parametrize(
        "form",
        [
            CollectionConfigForm(),
            CollectionConfigForm(
                collection_id=None,
                title="Foo is a Bar",
                description="Description of Foo",
            ),
            CollectionConfigForm(
                collection_id="foo",
                title=None,
                description="Description of Foo",
            ),
            CollectionConfigForm(
                collection_id="foo",
                title="Foo is a Bar",
                description=None,
            ),
        ],
    )
    def test_is_valid_returns_false(self, form):
        assert form.is_valid is False

    def test_validation_errors(self):
        form = CollectionConfigForm()
        form.collection_id = None
        form.title = "Foo is a Bar"
        form.description = "Description of Foo"
        form.keywords = ["foo", "bar", "oof"]
        form.providers = [ProviderModel(name="VITO", url="https://www.vito.be")]

        assert form.validation_errors


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


class TestInputsForm:
    def test_testinputsform(self):
        form = InputsForm()
        assert form.validation_errors
