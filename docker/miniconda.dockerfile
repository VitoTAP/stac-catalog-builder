FROM continuumio/miniconda3:23.10.0-1 as base

RUN --mount=type=cache,target=/opt/conda/pkgs \
    conda install --yes -c conda-forge mamba \
    && mamba init


FROM base as dependencies

ENV SRC_DIR=/src
WORKDIR ${SRC_DIR}
VOLUME ${SRC_DIR}}
ARG PYTHON_VERSION=3.11

RUN --mount=type=cache,target=/opt/conda/pkgs \
    mamba create --name testenv python=${PYTHON_VERSION}

COPY conda-environment.yaml ${SRC_DIR}
RUN --mount=type=cache,target=/var/cache/apt \
    mamba env create -f conda-environment.yaml


FROM dependencies as finalstage

COPY . ${SRC}

RUN --mount=type=cache,target=/var/cache/apt \
    mamba run -n stac-catalog-builder python3 -m pip install -e .

ENTRYPOINT ["mamba", "run", "-n", "stac-catalog-builder"]
CMD ["pytest", "-ra", "-vv"]
