FROM condaforge/miniforge3:23.11.0-0 as mambabase

# RUN --mount=type=cache,target=~/.cache/pip \
RUN --mount=type=cache,target=/opt/conda/pkgs \
    conda install --yes -c conda-forge mamba \
    && mamba init


from base as dependencies

ENV SRC_DIR=/src
WORKDIR ${SRC_DIR}
VOLUME ${SRC_DIR}}
ARG PYTHON_VERSION=3.11

RUN --mount=type=cache,target=/opt/conda/pkgs \
    mamba create --name testenv python=${PYTHON_VERSION}

# RUN --mount=type=cache,target=/opt/conda/pkgs \
#     mamba install --yes -n testenv -c conda-forge pystac[validation] stac-validator stactools rasterio shapely pyproj openeo click

COPY requirements/ ${SRC_DIR}/requirements

RUN --mount=type=cache,target=~/.cache/pip \
    python3 -m  pip install --no-cache-dir -r ${SRC_DIR}/requirements/requirements-dev.txt --extra-index-url https://artifactory.vgt.vito.be/artifactory/api/pypi/python-packages/simple


from dependencies as finalstage

COPY . ${SRC}

RUN --mount=type=cache,target=~/.cache/pip \
    python3 -m pip install -e .

ENTRYPOINT ["python3"]
CMD ["-m", "pytest", "-ra", "-vv"]
