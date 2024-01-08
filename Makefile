# ========================================================
# Configuration
# ========================================================



VENV_DIR := .venv

PIP_EXTRA_INDEX_URL := https://artifactory.vgt.vito.be/artifactory/api/pypi/python-packages/simple

# ========================================================
# Variables derived from configuration
# ========================================================
ifdef STACBLD_PYTHON_BIN
	PYTHON := $(STACBLD_PYTHON_BIN)
else
	PYTHON := $(VENV_DIR)/bin/python3
endif


# ========================================================
# make targets
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
	@echo "VENV_DIR=$(VENV_DIR)"
	@echo "PYTHON=$(PYTHON)"
	@echo "PIP_EXTRA_INDEX_URL=$(PIP_EXTRA_INDEX_URL)"
.PHONY: show-vars


piplist:
## Show the list of pip packages in the virtualenv
	$(PYTHON) -m pip list


create-venv:
## create an empty virtualenv.
	python3 -m venv $(VENV_DIR)
.PHONY: create-venv


bootstrap-venv:
## Bootstrap the virtualenv: install or update essential packages (but packages that should not be in requirements.txt)
	$(PYTHON) -m pip install -U pip setuptools wheel
.PHONY: bootstrap-venv


update-venv: bootstrap-venv
## Install/Update the dependencies in the virtualenv
	$(PYTHON)  -m pip install --extra-index-url $(PIP_EXTRA_INDEX_URL) -r requirements/requirements-dev.txt
	$(PYTHON)  -m pip install -e .
.PHONY: update-venv


test:
## Run pytest.
	$(PYTHON) -m pytest -ra -vv -l --ff

