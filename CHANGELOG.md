# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.1.0] - 
### Added
- Support for STAC 1.1 common `bands` in collection `item_assets` and generated asset metadata.

### Changed
- Upgrade to pystac 1.14.3, which includes support for STAC 1.1 common `bands` and deprecates `eo:bands` and `raster:bands`.
- [breaking] Asset config now supports a new `bands` field instead of eo_bands and raster_bands. eo and raster fields can be passed in this bands field.
- href's are now always stored as uri's. For files this means that they will be prefixed with `file://` and for s3 they will be prefixed with `s3://`. The original href is stored in the `original_href` field. This allows to keep the original path as it is, while still having a valid uri for pystac to work with.

### Removed
- Terracatalogueclient implementation and related dependency group. Running the stac builder on the source files has been added as a workflow.

### Fixed
- Small fixes for type hints

## [1.0.2] - 2026-02-19
### Added

- Streaming build path: `single_asset_per_item` mode lets metadata and STAC items flow without buffering everything in memory
- Default loguru setup (console + suppression of noisy libs) and log file support for workflows
- Pre-commit nbstripout hook for notebooks

### Changed

- Metadata collection uses streaming + throttled async pool (defaults to 10k outstanding tasks) with clearer progress logging ([#85](https://github.com/VitoTAP/stac-catalog-builder/issues/85))
- COG validation is cached per asset type to avoid repeated checks; catalog version strings standardized (e.g., `v01`/`v02`) ([#80](https://github.com/VitoTAP/stac-catalog-builder/issues/80))
- Uploading to a stac api now adjust asset href's to be uri's ([#97](https://github.com/VitoTAP/stac-catalog-builder/issues/97))

### Removed

### Fixed
- Added exception for when output directory ends with a suffix. pystac does not support this and will cause unexpected saving behavior.

## [1.0.1] - 2025-09-15
### Added

### Changed

- **BREAKING**: Replaced Python's standard `logging` module with `loguru` throughout the package
- Removed custom `_log_progress_message()` methods from classes (replaced with direct loguru calls)
- Limit the number of concurrent futures to avoid memory issues during metadata collection. Current setting is 1000. 
- Improve logging to show progress every 1000 files processed.

### Removed

### Fixed

- Fix for eo:bands and raster:bands from the config with tiffs that contain multiple bands. ([#76](https://github.com/VitoTAP/stac-catalog-builder/issues/76))
- Fix for rare issue where path parsers is not thread safe 

## [1.0.0] - 2025-08-11

⚠️ **BREAKING CHANGES**: This release contains major API changes that are not backward compatible. Most of these changes are related to [#53](https://github.com/VitoTAP/stac-catalog-builder/issues/53)

### Added

- Enhanced pre-commit configuration
- Comprehensive test suites with reference data validation
- Support for arguments in `fixed_values` config (e.g., `"fixed_values": {"item_id": "observations_{year}-{month}-{day}"}`)

### Changed

- **API Simplification**: Removed `GeoTiffPipeline` with simpler `AssetMetadataPipeline` as main entry point
- **Renamed classes**: `GeoTiffMetadataCollector` → `MetadataCollector`
- **AlternateHREF's function renames**: `add_MEP()` → `add_local()`, `add_basic_S3()` → `add_S3()`
- **Simplified imports**: Direct imports from `stacbuilder` package for cleaner API
- **Streamlined `AssetMetadata` class**: Simplified constructor and initialization
- **Code quality**: Applied ruff and black formatting, improved type hints and docstrings
- **Dependencies**: Moved to `pyproject.toml` for dependency management

### Removed

- `GeoTiffPipeline` class (use `AssetMetadataPipeline` instead)
- Collection configuration overrides system
- `PostProcessSTACCollectionFile` class
- Command-line interface support
- Legacy Makefile configurations
- Unused imports and dead code

### Fixed

- Issues with running the library in Jupyter notebooks ([#49](https://github.com/VitoTAP/stac-catalog-builder/issues/49))
- Cross-platform path handling throughout the codebase

---

## [0.1.0] - Previous Release

- Initial development release with basic STAC collection building functionality
- Support for GeoTIFF processing and metadata extraction
- Collection and item generation with validation
- Command-line interface for batch processing
- Support for grouped collections and complex workflows
