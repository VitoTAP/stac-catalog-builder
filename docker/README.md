# Dockerfiles for testing the package installation

First and foremost these dockerfiles are intended for testing the package installation, i.e.
do the pip requirements files and the `conda-environment.yaml` work correctly.

The idea is that if the installation succeeds and the test suite with pytest passes, > Tthan the requirements / conda env should be good enough for development.
This is not (yet) a production setup that should have locked down version to get reproducible builds.

Compare this to using `requirements.in` with pip-tools/pip-compile to get a fully resolved requirement.txt.
We are specifying and testing the first type of file here: `requirements.in`, not the requirement.txt.

> - TODO: (1) Maybe rename the requirements files for clarity: extension `.in` instead of `.txt`
> - TODO: (2) Decided if we want tooling to resolved full and locked dependencies, and which one: pip-tools, pip-env, poetry, or another,
> - TODO: (3) Can Hatch already do the job for `todo (2)`?

## Possible use in future: CI/testing/dev in docker container

That said, with some work you could also use these containers for testing or even developing.
We might be able to use it for CI / Jenkins in the near future.

Further, this may be useful if you are on Windows or Mac, because the tool is currently only being used and tested on Linux and this would give you a Linux environment.

Though for Windows, working in either a conda environment, or inside WSL, are another options and frankly that is less work than a Docker setup.

## Three environments to verify

There are three dockerfiles for three types of environments:

- [debianpython.dockerfile](debianpython.dockerfile): Uses only pip, in a Debian docker image.
- [miniconda.dockerfile](miniconda.dockerfile): Installs the packages in miniconda, starting from the [continuumio/miniconda3 image](https://hub.docker.com/r/continuumio/miniconda3).
- [miniforge.dockerfile](miniforge.dockerfile): Installs the packages in miniforge, the Open Source alternative to miniconda, and start from the [condaforge/miniforge3 docker image](https://hub.docker.com/r/condaforge/miniforge3)

## How to build the Docker images

### Example for debianpython.dockerfile

You can run the command inside the folder `docker` or you can run a slightly
different command in its parent folder, in other words in the root of your code repository.

There are two differences between those commands:

A) The path to the dockerfile itself
B) the so called "context" at the end is either the current folder `.`, or the parent folder `..`. The context must match where the code is located, to find the requirements files and the source code itself.

Pay close attention to the context because it is easy to overlook the small dot, or two dots, at the end of the command.

You can also use an absolute path for these parameters. We only chose to use the relative paths to keep the example shorter.

In the example we use ase the image name and tag: `stac-catalog-builder-debianpython:latest`, but you can use what you want off course.

A) Inside the the subdirectory `docker`, where the dockerfile itself is located:
    The value for "context" must be the parent folder in this case: so .. .

    ```bash
    cd docker

    docker build -t stac-catalog-builder-debianpython:latest -f debianpython.dockerfile ..
    ```


B) In the root of your git repository:
    The value for "context" must be the current directory, so `.`

    ```bash
    docker build -t stac-catalog-builder-debianpython:latest -f docker/debianpython.dockerfile .
    ```

## How to run the docker container

This runs the unit tests with pytest. If this succeeds your Docker environment should be OK.

Example for the example above with debianpython.dockerfile, using the same image name+tag
`stac-catalog-builder-debianpython:latest`.

```bash
docker run --rm -ti stac-catalog-builder-debianpython:latest
```

Open a bash shell inside the running docker container:

```bash
docker run --rm -ti --entrypoint bash stac-catalog-builder-debianpython:latest
```

## Examples for miniconda

### Build the image for miniconda

Image name and tag: `stac-catalog-builder-miniconda:latest`

A) Inside the the subdirectory `docker`

    ```bash
    docker build -t stac-catalog-builder-miniconda:latest -f miniconda.dockerfile ..
    ```


B) In the root of your git repository:
    The value for "context" must be the current directory, so `.`

    ```bash
    docker build -t stac-catalog-builder-miniconda:latest -f docker/miniconda.dockerfile .
    ```

### miniconda: Run the tests with the Docker container

```shell
docker run --rm -ti stac-catalog-builder-miniconda:latest
```

### miniconda: Open a bash shell inside the running docker container

```shell
docker run --rm -ti --entrypoint bash stac-catalog-builder-miniconda:latest
```

If you also want R/W access your source code inside the container, then you have to add `-v` option to add a bind-mount from your working directory to the folder with the source code. The source code is also the working directory defined in the docker image.

In Bash:

```bash
docker run --rm -ti -v ${PWD}:/src --entrypoint bash stac-catalog-builder-miniconda:latest
```

In Powershell:

```powershell
docker run --rm -ti -v ${pwd}:/src --entrypoint bash stac-catalog-builder-miniconda:latest
```
