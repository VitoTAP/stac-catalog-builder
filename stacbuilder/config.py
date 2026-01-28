"""
Model classes for the configuration of a STAC collection and all its components.

These are Pydantic model classes.

You can ignore the Form classes.
That idea didn't go very far and is likely to be removed at this point.
"""

import enum
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Union

from openeo.util import dict_no_none
from pydantic import BaseModel, ConfigDict, Field, HttpUrl
from pystac import MediaType
from pystac.extensions.eo import Band, EOExtension
from pystac.extensions.item_assets import AssetDefinition
from pystac.extensions.raster import RasterBand, RasterExtension
from pystac.provider import Provider, ProviderRole

DEFAULT_PROVIDER_ROLES: Set[ProviderRole] = [
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
        return Provider(name=self.name, url=self.url.unicode_string() if self.url else None, roles=list(self.roles))


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


class EOBandConfig(BaseModel):
    """Configuration for fields of the eo:bands element from the Electro-Optical STAC Extension.

    These fields have fixed values.

    See also Electro-Optical Extension Specification, band object:
    https://github.com/stac-extensions/eo#band-object
    """

    model_config = ConfigDict(from_attributes=True)

    name: str = Field(description="common_name of the band.")

    description: str = Field(description="Description of the band.")
    common_name: Optional[str] = Field(
        default=None, description="Common name of the band, such as 'red', 'green', 'blue', etc."
    )
    wavelength: Optional[float] = Field(
        default=None,
        description=("Wavelength of the band in nanometers. " + "This is a float value, e.g. 665.0 for the red band."),
    )

    def populate_asset_extension(self, ext: EOExtension) -> None:
        """Populate the EOExtension with the values from this configuration."""
        if not ext:
            return None
        eo_band = Band.create(
            name=self.name,
            common_name=self.common_name,
            description=self.description,
            center_wavelength=self.wavelength,
        )
        if not ext.bands:
            ext.apply(bands=[eo_band])
        else:
            ext.bands = ext.bands + [eo_band]


class SamplingType(enum.StrEnum):
    """Choices for the value of `sampling` in the RasterBand object of the Raster STAC extension

    This is used in `RasterBandConfig`, the configuration class for raster:band values.
    """

    AREA = "area"
    POINT = "point"


class RasterBandConfig(BaseModel):
    """Default values for the Raster Band Object from the Raster extension,
    i.e. the raster:band section in a STAC asset.

    See also: https://github.com/stac-extensions/raster
    """

    # not part of the extension but we need a a way to identify the band
    name: str

    # TODO: how do we store NaN in JSON?
    nodata: Optional[Union[int, float, str]] = Field(
        default=None,
        description=(
            "Pixel values used to identify pixels that are nodata in the band "
            + "either by the pixel value as a number or nan, inf or -inf (all strings).",
        ),
    )
    # TODO: maybe use a numpy type or make an Enum for data_type
    data_type: Optional[str] = Field(
        default=None,
        description=(
            "The data type of the pixels in the band. "
            + "One of the data types as described in this section of the "
            + "Raster Extension's README : "
            + "https://github.com/stac-extensions/raster#data-types"
        ),
    )

    sampling: Optional[str] = Field(
        default=SamplingType.AREA,
        description=(
            "One of area or point. "
            + "Indicates whether a pixel value should be assumed to represent a sampling "
            + "over the region of the pixel or a point sample at the center of the pixel."
        ),
        type=SamplingType,
    )

    bits_per_sample: Optional[int] = Field(
        default=None,
        description=(
            "The actual number of bits used for this band. "
            + "Normally only present when the number of bits is non-standard for "
            + "the datatype, such as when a 1 bit TIFF is represented as byte."
        ),
    )

    spatial_resolution: Optional[int] = Field(
        default=None, description="Average spatial resolution (in meters) of the pixels in the band."
    )

    unit: Optional[str] = Field(default=None, description="Unit denomination of the pixel value.")
    scale: Optional[Union[float, int]] = Field(
        default=None,
        description=(
            "Multiplicator factor of the pixel value to transform into the value "
            + "(i.e. translate digital number to reflectance)."
        ),
    )
    offset: Optional[Union[float, int]] = Field(
        default=None,
        description=(
            "Number to be added to the pixel value (after scaling) to transform "
            + "into the value (i.e. translate digital number to reflectance)."
        ),
    )

    def populate_asset_extension(self, ext: RasterExtension) -> None:
        """Populate the RasterExtension with the values from this configuration."""
        if not ext:
            return None
        raster_band = RasterBand.create(
            nodata=self.nodata,
            data_type=self.data_type,
            sampling=self.sampling,
            bits_per_sample=self.bits_per_sample,
            spatial_resolution=self.spatial_resolution,
            unit=self.unit,
            scale=self.scale,
            offset=self.offset,
        )
        if not ext.bands:
            ext.apply(bands=[raster_band])
        else:
            ext.bands = ext.bands + [raster_band]


class AssetConfig(BaseModel):
    """Configuration for the assets in a STAC item."""

    model_config = ConfigDict(from_attributes=True)

    title: str
    description: str
    media_type: Optional[MediaType] = MediaType.GEOTIFF
    roles: Optional[List[str]] = ["data"]

    # The bands are not always electro-optical bands,
    # for example weather observation and climate data.
    eo_bands: Optional[List[EOBandConfig]] = None

    raster_bands: Optional[List[RasterBandConfig]] = None

    def to_asset_definition(self) -> AssetDefinition:
        """Create an AssetDefinition object from this configuration."""
        if self.raster_bands:
            raster_bands = [dict_no_none(b.model_dump()) for b in self.raster_bands]
        else:
            raster_bands = None
        properties = {
            "type": self.media_type,
            "title": self.title,
            "description": self.description,
            "roles": self.roles,
        }
        asset_definition = AssetDefinition(properties=properties)
        if self.eo_bands:
            eo_ext = EOExtension.ext(asset_definition, add_if_missing=False)
            for eo_band in self.eo_bands:
                eo_band.populate_asset_extension(eo_ext)

        if raster_bands:
            raster_ext = RasterExtension.ext(asset_definition, add_if_missing=False)
            for raster_band in self.raster_bands:
                raster_band.populate_asset_extension(raster_ext)

        return asset_definition


class FileCollectorConfig(BaseModel):
    input_dir: Path
    glob: Optional[str] = "*"
    max_files: int = -1


class AssetHrefModifierConfig(BaseModel):
    url_template: str
    data_root: str


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
    item_assets: Optional[Dict[str, AssetConfig]] = {}

    asset_href_modifier: Optional[AssetHrefModifierConfig] = None
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
