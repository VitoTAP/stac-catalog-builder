
# ========================================================
# configuration
# ========================================================
VENV_DIR = .venv
PYTHON := $(VENV_DIR)/bin/python3


tiff_input_dir := tests/data/geotiff/mock-geotiffs
collection_config := tests/data/config/config-test-collection.json
output_dir := "tmp/test-output"



help:
## Show this help.
	@grep -A1 -e '^[a-z][a-zA-Z0-9\\\-]*:' $(MAKEFILE_LIST) | sed -s 's/^.*##/  /g' | sed -e 's/\-\-//g'
.PHONY: help

default: help
.PHONY: default



build-collection:
## Build the STAC collection
	$(PYTHON) stacbuilder \
		-v build \
		-g "*/*" \
		-c $(collection_config) \
		--overwrite \
		$(tiff_input_dir) \
		$(output_dir)


list-tiffs:
## List which GeoTIFF files the STAC builder finds with your current glob and input directory.
	$(PYTHON) stacbuilder \
		-v list-tiffs \
		-g "*/*" \
		$(tiff_input_dir)


list-metadata:
## For each file show what metadata it finds (Metadata not yet a STAC item, this is a step before that).
	$(PYTHON) stacbuilder \
		-v list-metadata \
		-g "*/*" \
		-c $(collection_config) \
		$(tiff_input_dir)


list-items:
## For each file show what the STAC Item looks like before it gets saved into the STAC collection.
	$(PYTHON) stacbuilder \
		-v list-items \
		-g "*/*" \
		-c $(collection_config) \
		$(tiff_input_dir)
