
# ========================================================
# configuration
# ========================================================
VENV_DIR = .venv

# ========================================================
# Variables derived from configuration
# ========================================================
ifdef STACBLD_PYTHON_BIN
	PYTHON := $(STACBLD_PYTHON_BIN)
else
	PYTHON := $(VENV_DIR)/bin/python3
endif

WORKSPACE := /home/johan.schreurs/Public/codebases/vito-git-repo/STAC-catalog-builder

COLLECTION_CONFIG := $(WORKSPACE)/tests/data/geotiff/mock-geotiffs
TIFF_INPUT_DIR := tests/data/config/config-test-collection.json
GLOB_PATTERN := "*/*.tif"

TEST_OUTPUT_DIR := $(WORKSPACE)/tmp/test-output
PUBLISH_DIR := $(WORKSPACE)/tmp/test-publish



help:
## Show this help.
	@grep -B1 -h -e '^\#\#' $(MAKEFILE_LIST) | sed -E -s 's/^## ([=\-])/\1/g' | sed -s 's/^##/  /g' | sed -e 's/^\-\-$$//g'
.PHONY: help

default: help
.PHONY: default



show-config:
## Show value of configuration variables and derived variables
	@echo "VENV_DIR=$(VENV_DIR)"
	@echo "PYTHON=$(PYTHON)"
	@echo "WORKSPACE=$(WORKSPACE)"
	@echo "COLLECTION_CONFIG=$(COLLECTION_CONFIG)"
	@echo "TIFF_INPUT_DIR=$(TIFF_INPUT_DIR)"
	@echo "GLOB_PATTERN=$(GLOB_PATTERN)"
	@echo "TEST_OUTPUT_DIR=$(TEST_OUTPUT_DIR)"
	@echo "PUBLISH_DIR=$(PUBLISH_DIR)"
.PHONY: show-config


build-collection:
## Build the STAC collection
	$(PYTHON) stacbuilder \
		-v build \
		-g "$(GLOB_PATTERN)" \
		-c $(COLLECTION_CONFIG) \
		--overwrite \
		$(TIFF_INPUT_DIR) \
		$(TEST_OUTPUT_DIR)


list-tiffs:
## List which GeoTIFF files the STAC builder finds with your current glob and input directory.
	$(PYTHON) stacbuilder \
		-v list-tiffs \
		-g "$(GLOB_PATTERN)" \
		$(TIFF_INPUT_DIR)


list-metadata:
## For each tiff file, show what the Metadata instance looks like. (Metadata not yet a STAC item, this is a step before that).
	$(PYTHON) stacbuilder \
		-v list-metadata \
		-g "$(GLOB_PATTERN)" \
		-m 20 \
		-c $(COLLECTION_CONFIG) \
		$(TIFF_INPUT_DIR)


list-items:
## For each tiff file, show what the STAC Item looks like.
	$(PYTHON) stacbuilder \
		-v list-items \
		-g "$(GLOB_PATTERN)" \
		-c $(COLLECTION_CONFIG) \
		$(TIFF_INPUT_DIR)


show-collection:
## Load and display the contents of the STAC collection file.
	$(PYTHON) stacbuilder show-collection "$(TEST_OUTPUT_DIR)/collection.json"


validate:
## Validate the contents of the STAC collection file.
	$(PYTHON) stacbuilder validate "$(TEST_OUTPUT_DIR)/collection-with-proj.json"



$(PUBLISH_DIR):
## Create the publish directory if it does not exist yet.
	mkdir -- "$(PUBLISH_DIR)"


publish-stac-catalog: $(PUBLISH_DIR)
## Copy the generated STAC files tot he publish directory.
	cp -r $(TEST_OUTPUT_DIR)/* $(PUBLISH_DIR)
	chgrp eodata --recursive $(PUBLISH_DIR)


ls-test-out:
## List the files in the test output directory.
	ls -la $(TEST_OUTPUT_DIR)


ls-publish-dir:
## List the files in the test pulbish directory.
	ls -la $(PUBLISH_DIR)


clean-test-out:
## Remove all files in the test output directory (if the directory is present).
##
## As a crude safety measure, you still have to copy the command and run it yourself.
## This is a clunky way to avoid accidental deletion on a potentially
## misconfigured directory. But this is temporary, until we have a more robust
## way to to let you check and confirm you really want to really delete the files.
	if [[ -d "$(TEST_OUTPUT_DIR)" ]] ;  then echo rm -r $(TEST_OUTPUT_DIR) ; else echo "test output dir does not exist: $(TEST_OUTPUT_DIR)" ; fi

clean-publish:
## Show the command that would remove all files in the test publish directory.
##
## As a crude safety measure, you still have to copy the command and run it yourself.
## This is a clunky way to avoid accidental deletion on a potentially
## misconfigured directory. But this is temporary, until we have a more robust
## way to to let you check and confirm you really want to really delete the files.
	echo rm -r $(PUBLISH_DIR)
	
	