FROM condaforge/miniforge3 as base

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


COPY requirements/ ${SRC_DIR}/requirements

RUN --mount=type=cache,target=/var/cache/apt \
    mamba run -n testenv python3 -m  pip install --no-cache-dir -r ${SRC_DIR}/requirements/requirements-dev.txt --extra-index-url https://artifactory.vgt.vito.be/artifactory/api/pypi/python-packages/simple


FROM dependencies as finalstage

COPY . ${SRC}

RUN --mount=type=cache,target=/var/cache/apt \
    mamba run -n testenv python3 -m pip install -e .

ENTRYPOINT ["mamba", "run", "-n", "testenv"]
CMD ["pytest", "-ra", "-vv"]
