# ==============================================================================
# WARNING:
# ------------------------------------------------------------------------------
# This makefile is really intended to be used with a regular virtualenv.
# Using this makefile in combination with conda is still *VERY* EXPERIMENTAL.
#
# You might break your conda environment.
#
# In general we want to avoid using pip with conda, but more work is needed to
# solve the remaining installation issues.
#
# ------------------------------------------------------------------------------
# TODO: [WIP] Maybe it would be a better safeguard to search for "conda" or "mamba" in the venv path and then refuse to create/update the venv.
# ==============================================================================


# ========================================================
# Configuration
# ========================================================

PIP_EXTRA_INDEX_URL := https://artifactory.vgt.vito.be/artifactory/api/pypi/python-packages/simple

# ========================================================
# Variables derived from configuration
# ========================================================

# Some safeguard againts running targets meant for virtualenvs inside a conda environment
# TODO: Consider simplifying the safeguard for conda envs. Could probably rely on STACBLD_VENV only and remove STACBLD_CONDA_ENV_DIR.
ifdef STACBLD_CONDA_ENV_DIR
	USING_CONDA := 1
	VENV_DIR := $(STACBLD_CONDA_ENV_DIR)
else
	USING_CONDA := 0
	ifdef STACBLD_VENV_DIR
		VENV_DIR := $(STACBLD_VENV_DIR)
	else
		VENV_DIR := .venv
	endif
	conda_occurences := $(findstring conda,$(VENV_DIR))
	mabda_occurences := $(findstring mamba,$(VENV_DIR))
	ifneq ( $(conda_occurences), )
		USING_CONDA := 1
	endif
	ifneq ( $(mabda_occurences), )
		USING_CONDA := 1
	endif
endif

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
	@echo "STACBLD_PYTHON_BIN=$(STACBLD_PYTHON_BIN)"
	@echo "STACBLD_CONDA_ENV_DIR=$(STACBLD_CONDA_ENV_DIR)"
	@echo "STACBLD_VENV_DIR=$(STACBLD_VENV_DIR)"
	@echo "VENV_DIR=$(VENV_DIR)"
	@echo "USING_CONDA=$(USING_CONDA)"
	@echo "PYTHON=$(PYTHON)"
	@echo "PIP_EXTRA_INDEX_URL=$(PIP_EXTRA_INDEX_URL)"
	$(PYTHON) -V
	$(PYTHON) -c "import sys; print(sys.executable)"
	@echo "conda_occurences=$(conda_occurences)"
	@echo "mabda_occurences=$(mabda_occurences)"
.PHONY: show-vars


piplist:
## Show the list of pip packages in the virtualenv
	$(PYTHON) -m pip list


create-venv:
## create an empty virtualenv (Unless you are using conda. Then we show an error instead).
ifeq ($(USING_CONDA), 0)
	python3 -m venv $(VENV_DIR)
else
	@echo "ERROR: You specified that are using conda for this project."
	@echo "       You should create the conda environment yourself via the command 'conda 'create', and then run the other make targets."
endif
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
