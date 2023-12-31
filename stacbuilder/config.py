"""
Model classes for the configuration of a STAC collection and all its components.

These are Pydantic model classes.

You can ignore the Form classes.
That idea didn't go very far and is likely to be removed at this point.
"""

import dataclasses as dc
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Union


import numpy as np
from pydantic_core import ErrorDetails
from pydantic import BaseModel, ConfigDict, HttpUrl, ValidationError
from pystac import MediaType
from pystac.provider import ProviderRole, Provider
from pystac.extensions.item_assets import AssetDefinition


from openeo.util import dict_no_none


DEFAULT_PROVIDER_ROLES: Set[ProviderRole] = {
    ProviderRole.PRODUCER,
    ProviderRole.LICENSOR,
    ProviderRole.PROCESSOR,
}


# TODO: [decide]: remove or not? Doesn't look like this BaseForm is the way to go.
class BaseForm:
    def __init__(self, model_cls: type):
        self._model_cls = model_cls

    def _get_model(self) -> BaseModel:
        """Convert to a Pydantic model, if the data is valid"""
        return self._model_cls.model_validate(self)

    @property
    def is_valid(self) -> bool:
        """Determine whether or not the data is valid"""
        try:
            self.get_model()
        except ValidationError as exc:
            return exc.errors()

    @property
    def validation_errors(self) -> List[ErrorDetails]:
        try:
            self.get_model()
        except ValidationError as exc:
            return exc.errors()
        else:
            return []


class ProviderModel(BaseModel):
    name: str
    # TODO: [decide]: us a set or just a list at the risk of having duplicates.
    roles: Set[ProviderRole] = DEFAULT_PROVIDER_ROLES
    url: Optional[HttpUrl] = None

    def to_provider(self) -> Provider:
        return Provider(name=self.name, url=self.url.unicode_string(), roles=list(self.roles))


class InputPathParserConfig(BaseModel):
    classname: str
    parameters: Optional[Dict[str, Any]] = None


class ItemConfig(BaseModel):
    description: str


class EOBandConfig(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    name: str
    description: str
    data_type: str

    # TODO: how do we store NaN in JSON?
    nodata: Optional[Union[int, float, str]] = None

    sampling: Optional[str] = None
    spatial_resolution: Optional[int] = None


class AssetConfig(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    title: str
    description: str
    media_type: Optional[MediaType] = MediaType.GEOTIFF
    roles: Optional[List[str]] = ["data"]
    eo_bands: List[EOBandConfig]
    # extra_fields = Dict[str, Any]

    def to_asset_definition(self) -> AssetDefinition:
        bands = [dict_no_none(b.model_dump()) for b in self.eo_bands]

        return AssetDefinition(
            properties={
                "type": self.media_type,
                "title": self.title,
                "description": self.description,
                "eo:bands": bands,
                "roles": self.roles,
            }
        )


class CollectionConfig(BaseModel):
    """Model, store configuration of a STAC collection"""

    model_config = ConfigDict(from_attributes=True)

    collection_id: str
    title: str
    description: str
    keywords: Optional[List[str]] = []
    providers: Optional[List[ProviderModel]]

    platform: Optional[List[str]] = []
    mission: Optional[List[str]] = []
    instruments: Optional[List[str]] = []

    layout_strategy_item_template: Optional[str] = "${collection}/${year}"
    input_path_parser: Optional[InputPathParserConfig] = None
    media_type: Optional[MediaType] = MediaType.GEOTIFF

    # Defines Items with their assets and what bands the assets contain.
    item_assets: Optional[Dict[str, AssetConfig]] = {}

    # TODO: links (urls)

    # A set of specific fields we want to give a fixed value at the end.
    # So this could override values that were generated.
    # For example I have used to to correct the collection's extent as a
    # temporary fix when there was something wrong, and to add a projected BBox as well.
    # This is done at the very end in the post-processing step of the builder.
    overrides: Optional[Dict[str, Any]] = None

    @classmethod
    def from_json_str(cls, json_str: str) -> "CollectionConfig":
        return CollectionConfig.model_validate_json(json_str)

    @classmethod
    def from_json_file(cls, path: str | Path) -> "CollectionConfig":
        cfg_path = Path(path)
        contents = cfg_path.read_text()
        return cls.from_json_str(contents)


@dc.dataclass
class CollectionConfigForm:
    """Allows you to fill in a configuration of a STAC collection.

    While it is very similar to CollectionConfig, a Pydantic model instance
    can not be created with invalid data which is very annoying in a UI
    but a form will let you fill in that data before the application
    validates and converts it to the model, a CollectionConfig instance.
    """

    collection_id: Optional[str] = None
    title: Optional[str] = None
    description: Optional[str] = None
    keywords: Optional[List[str]] = dc.field(default_factory=list)
    providers: Optional[List[ProviderModel]] = dc.field(default_factory=list)

    platform: Optional[List[str]] = dc.field(default_factory=list)
    mission: Optional[List[str]] = dc.field(default_factory=list)
    instruments: Optional[List[str]] = dc.field(default_factory=list)

    layout_strategy_item_template: Optional[str] = "${collection}/${year}"
    input_path_parser: Optional[Dict[str, Any]] = None
    media_type: Optional[MediaType] = MediaType.GEOTIFF

    # Implementing item_assets would require a form for the AssetConfig.
    # item_assets: Optional[Dict[str, AssetConfig]] = {}

    overrides: Optional[Dict[str, Any]] = None

    # TODO: links (urls)

    def get_model(self) -> CollectionConfig:
        """Convert to a  CollectionConfig, if the data is valid"""
        return CollectionConfig.model_validate(self)

    @property
    def is_valid(self) -> bool:
        """Determine whether or not the data is valid"""
        try:
            self.get_model()
            return True
        except ValidationError:
            return False

    @property
    def validation_errors(self) -> List[ErrorDetails]:
        try:
            self.get_model()
        except ValidationError as exc:
            return exc.errors()
        else:
            return []

    @classmethod
    def from_json_str(cls, json_str: str) -> CollectionConfig:
        return CollectionConfig.model_validate_json(json_str)

    @classmethod
    def from_json_file(cls, path: Path) -> CollectionConfig:
        contents = path.read_text()
        return cls.from_json_str(contents)


class InputsModel(BaseModel):
    input_dir: Path
    glob: Optional[str] = "*"


@dc.dataclass
class InputsForm:
    input_dir: Optional[Path] = None
    glob: Optional[str] = "*"

    def get_model(self) -> InputsModel:
        """Convert to a InputsModel, if the data is valid"""
        return InputsModel.model_validate(self)

    @property
    def validation_errors(self) -> List[ErrorDetails]:
        errors = []
        try:
            self.get_model()
        except ValidationError as exc:
            errors = exc.errors()

        if self.input_dir and not self.input_dir.exists():
            errors.append(f"Input directory does not exist: {self.input_dir!r}")

        return errors

    @property
    def is_valid(self):
        return not self.get_errors()


@dc.dataclass
class BuilderInputOutputForm:
    input_dir: Path
    output_dir: Path
    glob: Optional[str] = "*"

    # def check_paths(self):
    #     pass


class STACBuilderConfig(BaseModel):
    input_dir: Path
    output_dir: Path
    glob: Optional[str] = "*"
