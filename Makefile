# ========================================================
# Configuration
# ========================================================

# PIP_EXTRA_INDEX_URL := https://artifactory.vgt.vito.be/api/pypi/python-packages/simple

# ========================================================
# Variables derived from configuration
# ========================================================

PROJECT_NAME := stac-catalog-builder
VENV_NAME := $(realpath .venv)
# VENV_DIR := $(VIRTUALENVS_DIR)/$(VENV_NAME)
VENV_DIR := $(VENV_NAME)


# We don't want realpath for python3 because there are symlinks involved and 
# then we get the executable in .pyenv/versions/<pyversion>/bin instead of the
# one we want, inside the virtualenv.
PYTHON := $(VENV_DIR)/bin/python3
# PYTHON_REALPATH := $(realpath $(VENV_DIR)/bin/python3)
PYTHON_REALPATH := $(realpath ${PYTHON})



tiff_input_dir := tests/data/geotiff/mock-geotiffs
collection_config := tests/data/config/config-test-collection.json
output_dir := "tmp/test-output"

# ========================================================


default: help
.PHONY: default

all: help
.PHONY: all

help:
## Show this help.
	@grep -A1 -e '^[a-z][a-zA-Z0-9\\\-]*:' $(MAKEFILE_LIST) | sed -s 's/^##/  /g' | sed -e 's/\-\-//g'
.PHONY: help


show-config:
## Show value of configuration variables
	@echo "VIRTUALENVS_DIR=$(VIRTUALENVS_DIR)"
	@echo "PIP_EXTRA_INDEX_URL=$(PIP_EXTRA_INDEX_URL)"
.PHONY: show-config

show-vars: show-config
## Show value of configuration variables and derived variables
	@echo "VENV_NAME=$(VENV_NAME)"
	@echo "VENV_DIR=$(VENV_DIR)"
	@echo "PYTHON=$(PYTHON)"
	@echo "PYTHON_REALPATH=$(PYTHON_REALPATH)"
.PHONY: show-vars


piplist:
	$(PYTHON) -m pip list


create-venv:
## create an empty virtualenv
	python3 -m venv $(VENV_DIR)
.PHONY: create-venv


bootstrap-venv:
	$(PYTHON) -m pip install -U pip setuptools wheel
.PHONY: bootstrap-venv


update-venv: bootstrap-venv
	$(PYTHON)  -m pip install -r requirements/requirements-dev.txt
	$(PYTHON)  -m pip install -e .
.PHONY: update-venv


test:
## Run pytest.
	$(PYTHON) -m pytest -ra -vv -l --ff


list-tiffs:
	$(PYTHON) stacbuilder \
		-v list-tiffs \
		-g "*/*" \
		$(tiff_input_dir)

list-metadata:
	$(PYTHON) stacbuilder \
		-v list-metadata \
		-g "*/*" \
    	-c $(collection_config) \
		$(tiff_input_dir)

list-items:
	$(PYTHON) stacbuilder \
		-v list-items \
		-g "*/*" \
    	-c $(collection_config) \
		$(tiff_input_dir)

build-collection:
	$(PYTHON) stacbuilder \
		-v build \
		-g "*/*" \
    	-c $(collection_config) \
		--overwrite \
		$(tiff_input_dir) \
		$(output_dir)
