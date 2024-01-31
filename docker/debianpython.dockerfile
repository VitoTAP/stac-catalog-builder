# syntax=docker/dockerfile:1.2

# TODO: Add build-arg for python version
# TODO: FIX: would prefer the slim version "python:3.8-slim-bookworm" but then gdal wheels can't be built.
# At the time of writing, the latest debian version was: debian 12 bookworm
FROM python:3.11-bookworm AS base

# TODO: [1] should these env vars become an ARG so we can override it in docker build?
# ARG GDAL_VERSION=3.6.2

# TODO: Maybe this patter is better for TODO[1]: provide a build arg and put it in an ENV
ARG BUILDARG_GDAL_VERSION=3.6.2
ENV GDAL_VERSION=${BUILDARG_GDAL_VERSION}

ARG BUILDARG_JAVA_VERSION=17
ENV JAVA_VERSION=${BUILDARG_JAVA_VERSION}

ENV JAVA_HOME=/usr/lib/jvm/java-${JAVA_VERSION}-openjdk-amd64

# Keeps Python from generating .pyc files in the container
ENV PYTHONDONTWRITEBYTECODE=1

# Turns off buffering for easier container logging
ENV PYTHONUNBUFFERED=1

# Source directory, for source code (or app directory if you will)
ENV SRC_DIR=/src
WORKDIR $SRC_DIR
VOLUME $SRC_DIR

COPY docker/apt-install-dependencies.sh .

#
# For all RUN statements that use --mount:
#   Keep --mount options on the first line, and put all bash commands
#   on the lines below.
#   This is necessary to be able to autogenerate minimal-no-buildkit.dockerfile
#   from minimal.dockerfile with an automated find-replace of the RUN statement.
#
RUN --mount=type=cache,target=/var/cache/apt/ \
    ./apt-install-dependencies.sh

# TODO: DECIDE: pip package versions for reproducibility or prefer to have the latest, at least for pip?
#   normally it is best to pin package versions for reproducibility but maybe not for pip.
RUN --mount=type=cache,target=~/.cache/pip \
    python3 -m pip install --no-cache-dir --upgrade pip==23.3 wheel==0.41 pip-tools==7.3


# GDAL is always a difficult one to install properly so we do this early on.
RUN --mount=type=cache,target=~/.cache/pip \
    python3 -m pip install --no-cache-dir pygdal=="$(gdal-config --version).*"


from base as dependencies

ENV SRC_DIR=/src
WORKDIR ${SRC_DIR}
VOLUME ${SRC_DIR}}

COPY requirements/ ${SRC_DIR}/requirements

RUN --mount=type=cache,target=~/.cache/pip \
    python3 -m  pip install --no-cache-dir -r ${SRC_DIR}/requirements/requirements-dev.txt --extra-index-url https://artifactory.vgt.vito.be/artifactory/api/pypi/python-packages/simple



from dependencies as finalstage

COPY . ${SRC}

RUN --mount=type=cache,target=~/.cache/pip \
    python3 -m  pip install -e .

ENTRYPOINT ["python3"]
CMD ["-m", "pytest", "-ra", "-vv"]
