"""
Model classes for the configuration of a STAC collection and all its components.

These are Pydantic model classes.

You can ignore the Form classes.
That idea didn't go very far and is likely to be removed at this point.
"""

import enum
import warnings
from pathlib import Path
from typing import Any, ClassVar, Dict, List, Optional, Set, Union

from openeo.util import dict_no_none
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    HttpUrl,
    field_validator,
    model_validator,
)
from pystac import ItemAssetDefinition, MediaType
from pystac.provider import Provider, ProviderRole

DEFAULT_PROVIDER_ROLES: List[ProviderRole] = [
    ProviderRole.PRODUCER,
    ProviderRole.LICENSOR,
    ProviderRole.PROCESSOR,
]


class ProviderModel(BaseModel):
    """Model for Providers in STAC."""

    name: str
    roles: List[ProviderRole] = DEFAULT_PROVIDER_ROLES
    url: Optional[HttpUrl] = None

    def to_provider(self) -> Provider:
        return Provider(name=self.name, url=self.url.unicode_string(), roles=list(self.roles))


class InputPathParserConfig(BaseModel):
    """Configuration for the InputPathParser,
    which parses the paths of input files to extract metadata from the path.

    Which class to instantiate, and optionally, which parameters to pass to
    its constructor.
    """

    classname: str
    parameters: Optional[Dict[str, Any]] = None


class ItemConfig(BaseModel):
    """Configuration for fixed-value fields of STAC items.

    This is mainly intended for fields that we can not automatically extract
    from the raster/source data.
    """

    description: str


class SamplingType(enum.StrEnum):
    """Choices for the value of `sampling` in the RasterBand object of the Raster STAC extension

    This is used in `BandConfig`, for the raster:sampling field.
    """

    AREA = "area"
    POINT = "point"


class BandConfig(BaseModel):
    """Configuration model for common band objects."""

    model_config = ConfigDict(populate_by_name=True)

    ALLOWED_DATA_TYPES: ClassVar[Set[str]] = {
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

    name: str
    description: Optional[str] = None

    nodata: Optional[Union[int, float, str]] = None
    data_type: Optional[str] = None
    unit: Optional[str] = None

    eo_common_name: Optional[str] = Field(default=None, alias="eo:common_name")
    eo_center_wavelength: Optional[float] = Field(default=None, alias="eo:center_wavelength")

    raster_sampling: Optional[str] = Field(default=None, alias="raster:sampling", type=SamplingType)
    raster_bits_per_sample: Optional[int] = Field(default=None, alias="raster:bits_per_sample")
    raster_spatial_resolution: Optional[int] = Field(default=None, alias="raster:spatial_resolution")
    raster_scale: Optional[Union[float, int]] = Field(default=None, alias="raster:scale")
    raster_offset: Optional[Union[float, int]] = Field(default=None, alias="raster:offset")

    @field_validator("data_type")
    @classmethod
    def _validate_data_type(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return value

        data_type = value.strip().lower()
        if data_type not in cls.ALLOWED_DATA_TYPES:
            allowed = ", ".join(sorted(cls.ALLOWED_DATA_TYPES))
            warnings.warn(
                f"Invalid data_type '{value}'. Falling back to 'other'. Expected one of: {allowed}",
                UserWarning,
                stacklevel=2,
            )
            return "other"
        return data_type

    @model_validator(mode="before")
    @classmethod
    def _validate_common_field_prefixes(cls, data: Any) -> Any:
        if not isinstance(data, dict):
            return data

        invalid_keys = {"eo:name", "raster:name", "eo:description", "raster:description"}
        found = sorted(k for k in invalid_keys if k in data)
        if found:
            raise ValueError(
                "Band common fields must be unprefixed: use 'name' and 'description', not " + ", ".join(found)
            )
        return data

    def to_common_band_dict(self) -> Dict[str, Any]:
        """Convert to a STAC 1.1 common band dictionary."""
        return dict_no_none(
            {
                "name": self.name,
                "description": self.description,
                "nodata": self.nodata,
                "data_type": self.data_type,
                "unit": self.unit,
                "eo:common_name": self.eo_common_name,
                "eo:center_wavelength": self.eo_center_wavelength,
                "raster:sampling": self.raster_sampling,
                "raster:bits_per_sample": self.raster_bits_per_sample,
                "raster:spatial_resolution": self.raster_spatial_resolution,
                "raster:scale": self.raster_scale,
                "raster:offset": self.raster_offset,
            }
        )


class AssetConfig(BaseModel):
    """Configuration for the assets in a STAC item."""

    model_config = ConfigDict(from_attributes=True)

    title: str
    description: str
    media_type: Optional[MediaType] = MediaType.GEOTIFF
    roles: Optional[List[str]] = ["data"]

    # The bands are not always electro-optical bands,
    # for example weather observation and climate data.
    bands: Optional[List[BandConfig]] = None

    @model_validator(mode="before")
    @classmethod
    def _migrate_legacy_band_blocks(cls, data: Any) -> Any:
        if not isinstance(data, dict):
            return data

        if "eo_bands" not in data and "raster_bands" not in data:
            return data

        migrated = dict(data)
        merged_by_name: Dict[str, Dict[str, Any]] = {}

        for band in migrated.get("bands") or []:
            if not isinstance(band, dict):
                continue
            band_name = band.get("name")
            if band_name:
                merged_by_name[band_name] = dict(band)

        for eo_band in migrated.get("eo_bands") or []:
            if not isinstance(eo_band, dict):
                continue
            band_name = eo_band.get("name")
            if not band_name:
                continue
            current = merged_by_name.get(band_name, {"name": band_name})
            updated = {
                **current,
                "name": band_name,
                "description": eo_band.get("description", current.get("description")),
                "eo:common_name": eo_band.get("common_name", current.get("eo:common_name")),
                "eo:center_wavelength": eo_band.get("wavelength", current.get("eo:center_wavelength")),
            }
            merged_by_name[band_name] = updated

        for raster_band in migrated.get("raster_bands") or []:
            if not isinstance(raster_band, dict):
                continue
            band_name = raster_band.get("name")
            if not band_name:
                continue
            current = merged_by_name.get(band_name, {"name": band_name})
            updated = {
                **current,
                "name": band_name,
                "nodata": raster_band.get("nodata", current.get("nodata")),
                "data_type": raster_band.get("data_type", current.get("data_type")),
                "unit": raster_band.get("unit", current.get("unit")),
                "raster:sampling": raster_band.get("sampling", current.get("raster:sampling")),
                "raster:bits_per_sample": raster_band.get("bits_per_sample", current.get("raster:bits_per_sample")),
                "raster:spatial_resolution": raster_band.get(
                    "spatial_resolution", current.get("raster:spatial_resolution")
                ),
                "raster:scale": raster_band.get("scale", current.get("raster:scale")),
                "raster:offset": raster_band.get("offset", current.get("raster:offset")),
            }
            merged_by_name[band_name] = updated

        migrated["bands"] = list(merged_by_name.values())
        migrated.pop("eo_bands", None)
        migrated.pop("raster_bands", None)
        return migrated

    def get_common_bands(self) -> Optional[List[Dict[str, Any]]]:
        """Return STAC 1.1 common bands from the unified `bands` config."""
        if not self.bands:
            return None
        return [band.to_common_band_dict() for band in self.bands]

    def uses_raster_extension(self) -> bool:
        """Check if this config uses raster-prefixed band fields."""
        for band in self.bands or []:
            band_dict = band.to_common_band_dict()
            if any(key.startswith("raster:") for key in band_dict):
                return True
        return False

    def uses_eo_extension(self) -> bool:
        """Check if this config uses eo-prefixed band fields."""
        for band in self.bands or []:
            band_dict = band.to_common_band_dict()
            if any(key.startswith("eo:") for key in band_dict):
                return True
        return False

    def to_asset_definition(self) -> ItemAssetDefinition:
        """Create an ItemAssetDefinition object from this configuration."""
        common_bands = self.get_common_bands()
        properties = {
            "type": self.media_type,
            "title": self.title,
            "description": self.description,
            "roles": self.roles,
        }
        if common_bands:
            properties["bands"] = common_bands
        asset_definition = ItemAssetDefinition(properties=properties)

        return asset_definition


class FileCollectorConfig(BaseModel):
    input_dir: Path
    glob: Optional[str] = "*"
    max_files: int = -1


class AlternateHrefConfig(BaseModel):
    """Configuration for what alternate links we need to add.

    This implementation is simple but only makes it possible to add pre-defined
    alternates that we know how to set up. But that is enough for now.
    We could make it possible to register new subclasses of AlternateLinksGenerator
    but that is a bit more complex and we don't need it now.

    So taking the simple and direct approach until we need more.

    See also: stacbuilder.builder.AlternateLinksGenerator
    In particular these methods:
    - AlternateLinksGenerator.from_config
    - AlternateLinksGenerator.add_local
    - AlternateLinksGenerator.add_S3
    """

    add_local: bool = True
    add_S3: bool = False
    s3_bucket: Optional[str] = None
    s3_root_path: Optional[str] = None


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

    # layout strategy: defines what sub folders are created to save the STAC items, relative to the collection.json
    # See also: https://pystac.readthedocs.io/en/stable/api/layout.html#pystac.layout.TemplateLayoutStrategy
    layout_strategy_item_template: Optional[str] = "${collection}/${year}"

    input_path_parser: Optional[InputPathParserConfig] = None

    # What the media type is of the raster files, typically either MediaType.GEOTIFF or MediaType.COG.
    media_type: Optional[MediaType] = MediaType.GEOTIFF

    # Defines what assets items have, and what bands the assets contain.
    item_assets: Dict[str, AssetConfig] = {}

    alternate_links: Optional[AlternateHrefConfig] = None

    @classmethod
    def from_json_str(cls, json_str: str) -> "CollectionConfig":
        return CollectionConfig.model_validate_json(json_str)

    @classmethod
    def from_json_file(cls, path: str | Path) -> "CollectionConfig":
        cfg_path = Path(path)
        contents = cfg_path.read_text()
        return cls.from_json_str(contents)


class GeotTIFFPipelineConfig(BaseModel):
    collection_config: CollectionConfig
    input_files_config: FileCollectorConfig
    output_dir: Path


class OpenSearchPipelineConfig(BaseModel):
    collection_config: CollectionConfig
    # ??? Don't know yet what setup we need to connect to OpenSearch and ingest this input.

    # for now: output it as static STAC collection
    # This will be replaced with settings to upload it to a STAC API.
    output_dir: Path
    max_products: Optional[int] = -1
