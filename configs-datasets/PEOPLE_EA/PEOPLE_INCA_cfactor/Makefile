# ==============================================================================
# Configuration
# ==============================================================================
# CHANGE THESE FOR A NEW DATASET
# Meaning of each variable is explained above each variable definition
#
# General settings:
# - STACBLD_PYTHON_BIN
# - WORKSPACE
#
# Settings about the input:
# - DATASET_NAME
# - 
#
# Settings about the output:
# - OUT_DIR_ROOT
# - PUBLISH_DIR_ROOT
# - CATALOG_VERSION
# ==============================================================================


# ------------------------------------------------------------------------------
# STACBLD_PYTHON_BIN
# ------------------------------------------------------------------------------
# Absolute path to the python executable you are using.
#
# This is necessary because of the inflexible difficult setup on Terrascope
# Watch out: on Terrascope the command `python`is really an alias to the
# OS's python which is v3.6 and you should not use that one.
STACBLD_PYTHON_BIN := /home/johan.schreurs/mambaforge/envs/mf_stac-catalog-builder/bin/python3


# It is best to set STACBLD_PYTHON_BIN, but we can fall back to a local
# virtualenv if there is one.
# However, on Terrascope it is very difficult to create working virtualenvs,
# because some Python packages depend on OS-packges, and the yum package repo
# does not have up to date versions of those depencies.
# Using conda is the only way on Terrascope.
VENV_DIR = .venv

ifdef STACBLD_PYTHON_BIN
	PYTHON := $(STACBLD_PYTHON_BIN)
else
	PYTHON := $(VENV_DIR)/bin/python3
endif


# ------------------------------------------------------------------------------
# WORKSPACE
# ------------------------------------------------------------------------------
# Your root folder for saving STAC files or other data.
# 
# Some settings for other paths are derived from WORKSPACE.
#
# This is not the final output but a place to generate and review before you
# "publish" it when you got a good version.
# While this makes the workflow more complex, I prefer to develop and test in
# a separate directory before I hand it over to to the user.
# Otherwise it can become a mess.


#
# You can also set WORKSPACE to a fixed value but.
# However the template will search and save everything in the same directory as the Makefile.
#
# WORKSPACE := /home/johan.schreurs/Public/codebases/vito-git-repo/STAC-catalog-builder/configs-datasets/PEOPLE_EA/PEOPLE_INCA_cfactor/

#
# Set workspace to directory where this Makefile is located.
#
MK_PATH = $(abspath $(lastword $(MAKEFILE_LIST)))
# MK_PARENT = $(realpath $(MK_PATH)../)
MK_PARENT = $(dir $(realpath $(MK_PATH)))
WORKSPACE = $(realpath  $(MK_PARENT)../)

# ------------------------------------------------------------------------------
# DATASET_NAME
# ------------------------------------------------------------------------------
# Just the name of you collection.
#
# Typically the name of the folder where you read the input files. 
# ------------------------------------------------------------------------------
DATASET_NAME := PEOPLE_INCA_cfactor


# ------------------------------------------------------------------------------
# COLLECTION_CONFIG
# ------------------------------------------------------------------------------
# This is a file you need to fill in so the STACBuilder knows what to fill in.
#
# You should find it in the same folder as this Makefile.
#
# TODO: simplify: remove ".local/datasets/PEOPLE_EA"
COLLECTION_CONFIG := $(WORKSPACE)/${DATASET_NAME}/config-collection.json


# ------------------------------------------------------------------------------
# TIFF_INPUT_DIR
# ------------------------------------------------------------------------------
# Directory containing the GeoTIFF input files.
#
# This can be one of its parents / ancesters, because we also use a glob pattern
# to find the files. This allows us to work with files that are grouped per
# per, or per date.
TIFF_INPUT_DIR := /data/MTDA/PEOPLE_EA/PEOPLE_INCA_cfactor/


# ------------------------------------------------------------------------------
# GLOB_PATTERN
# ------------------------------------------------------------------------------
# Glob pattern to find the GeoTIFF files inside TIFF_INPUT_DIR.
GLOB_PATTERN := "*.tif"


# ------------------------------------------------------------------------------
# CATALOG_VERSION
# ------------------------------------------------------------------------------
# Name for a version subfolder for the version/revision of the STAC collection.
# 
# To be able to compare with the previously generated STAC, you may want to have 
# a subfolder with a simple version number.
# This is not a hard requirement but often very handy when you are trying to
# correct problems.
CATALOG_VERSION := v0.3

# ------------------------------------------------------------------------------
# OUT_DIR_ROOT
# ------------------------------------------------------------------------------
# Base/root directort for the test output.
# This is where you test and review the STAC files, so this is not the PUBLISH_DIR
OUT_DIR_ROOT := $(WORKSPACE)/${DATASET_NAME}

# ------------------------------------------------------------------------------
# OPENEO_BACKEND_URL 
# ------------------------------------------------------------------------------
# URL for the open-EO backend to use when testing the STAC collection in
# open-EO (via the python client) 
OPENEO_BACKEND_URL = openeo-dev.vito.be


#
# Derived variables below
#

TEST_OUTPUT_DIR := $(OUT_DIR_ROOT)/STAC_wip/$(CATALOG_VERSION)


# The normal way to configure the PUBLISH_DIR and PUBLISH_DIR_ROOT
# In the template file it is set to a different safe value that is inside the local git repo.
# PUBLISH_DIR_ROOT := /data/users/Public/johan.schreurs/PEOPLE_EA/STAC-for-review

# Use this pattern with PUBLISH_DIR_ROOT set to an absolute path (not derived from other variable) 
# PUBLISH_DIR := $(PUBLISH_DIR_ROOT)/${DATASET_NAME}/$(CATALOG_VERSION)


#
# Safe test values for PUBLISH_DIR / PUBLISH_DIR_ROOT
#
# This is a safe value in the local folder for testing.
# Basically this doens not actually publish it, but you can be sure the folder
# exists and it is inside the local git working directory.
PUBLISH_DIR_ROOT := $(OUT_DIR_ROOT)/STAC_publish
PUBLISH_DIR := $(OUT_DIR_ROOT)/STAC_publish/$(CATALOG_VERSION)


TEST_OPENEO_DIR = $(OUT_DIR_ROOT)/openeo-test-out


# ==============================================================================
# make targets
# ==============================================================================

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
	@echo "MK_PARENT=$(MK_PARENT)"
	@echo "MK_PATH=$(MK_PATH)"
	@echo "COLLECTION_CONFIG=$(COLLECTION_CONFIG)"
	@echo "TIFF_INPUT_DIR=$(TIFF_INPUT_DIR)"
	@echo "GLOB_PATTERN=$(GLOB_PATTERN)"
	@echo "TEST_OUTPUT_DIR=$(TEST_OUTPUT_DIR)"
	@echo "TEST_OPENEO_DIR=$(TEST_OPENEO_DIR)"
	@echo "PUBLISH_DIR=$(PUBLISH_DIR)"
.PHONY: show-config



.PHONY: build-collection
build-collection:
## Build the STAC collection
	$(PYTHON) stacbuilder \
		-v build \
		-g "$(GLOB_PATTERN)" \
		-c $(COLLECTION_CONFIG) \
		--overwrite \
		$(TIFF_INPUT_DIR) \
		$(TEST_OUTPUT_DIR)



.PHONY: list-tiffs
list-tiffs:
## List which GeoTIFF files the STAC builder finds with your current glob and input directory.
	$(PYTHON) stacbuilder \
		-v list-tiffs \
		-g "$(GLOB_PATTERN)" \
		$(TIFF_INPUT_DIR)



.PHONY: list-metadata
list-metadata:
## For each tiff file, show what the Metadata instance looks like. (Metadata not yet a STAC item, this is a step before that).
	$(PYTHON) stacbuilder \
		-v list-metadata \
		-g "$(GLOB_PATTERN)" \
		-m 20 \
		-c $(COLLECTION_CONFIG) \
		--save-dataframe \
		$(TIFF_INPUT_DIR)



.PHONY: list-items
list-items:
## For each tiff file, show what the STAC Item looks like.
	$(PYTHON) stacbuilder \
		-v list-items \
		-g "$(GLOB_PATTERN)" \
		-c $(COLLECTION_CONFIG) \
		--save-dataframe \
		$(TIFF_INPUT_DIR)



.PHONY: show-collection
show-collection:
## Load and display the contents of the STAC collection file.
	$(PYTHON) stacbuilder show-collection "$(TEST_OUTPUT_DIR)/collection.json"


.PHONY: validate
validate:
## Validate the contents of the STAC collection file.
	$(PYTHON) stacbuilder validate "$(TEST_OUTPUT_DIR)/collection.json"


.PHONY: test-openeo
test-openeo:
## Test the STAC collection works in open-EO with load_stac by downloading a small part of it.
	$(PYTHON) stacbuilder test-openeo \
	-v \
	-b $(OPENEO_BACKEND_URL) \
	-o $(TEST_OPENEO_DIR) \
	--max-extent-size=2000 \
	"$(PUBLISH_DIR)/collection.json"


.PHONY: test-openeo-dryrun
test-openeo-dryrun:
## Dry-run show what extent / config would be uses to test the STAC collection 
## in open-EO with load_stac by downloading a small part of it.
	$(PYTHON) stacbuilder test-openeo \
	-v \
	--dry-run \
	-b $(OPENEO_BACKEND_URL) \
	-o $(TEST_OPENEO_DIR) \
	--max-extent-size=2000 \
	"$(PUBLISH_DIR)/collection.json"


$(PUBLISH_DIR):
## Create the publish directory if it does not exist yet.
	mkdir -- "$(PUBLISH_DIR)"


.PHONY: publish-stac-catalog
publish-stac-catalog: $(PUBLISH_DIR)
## Copy the generated STAC files tot he publish directory.
	cp -r $(TEST_OUTPUT_DIR)/* $(PUBLISH_DIR)
	chgrp eodata --recursive $(PUBLISH_DIR)


.PHONY: ls-test-out
ls-test-out:
## List the files in the test output directory.
	ls -la $(TEST_OUTPUT_DIR)



.PHONY: ls-publish-dir
ls-publish-dir:
## List the files in the test pulbish directory.
	ls -la $(PUBLISH_DIR)


.PHONY: clean-test-out
clean-test-out:
##
## Remove all files in the test output directory (if the directory is present).
##
## As a crude safety measure, you still have to copy the command and run it yourself.
## This is a clunky way to avoid accidental deletion on a potentially
## misconfigured directory. But this is temporary, until we have a more robust
## way to to let you check and confirm you really want to really delete the files.
	if [[ -d "$(TEST_OUTPUT_DIR)" ]] ;  then echo rm -r $(TEST_OUTPUT_DIR) ; else echo "test output dir does not exist: $(TEST_OUTPUT_DIR)" ; fi


.PHONY: clean-publish
clean-publish:
##
## Show the command that would remove all files in the test publish directory.
##
## As a crude safety measure, you still have to copy the command and run it yourself.
## This is a clunky way to avoid accidental deletion on a potentially
## misconfigured directory. But this is temporary, until we have a more robust
## way to to let you check and confirm you really want to really delete the files.
	echo rm -r $(PUBLISH_DIR)

