# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.0.1] - in progress
### Added
- 

### Changed

- Limit the number of concurrent futures to avoid memory issues during metadata collection. Current setting is 1000. 
- Improve logging to show progress every 1000 files processed.

### Removed
- 

### Fixed

- Fix for eo:bands with tiffs that contain multiple bands. ([#76](https://github.com/VitoTAP/stac-catalog-builder/issues/76))
- Fix for rare issue where pathparsing is not thread safe 


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
