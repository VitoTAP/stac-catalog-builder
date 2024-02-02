# Dockerfiles for testing the package installation

- [Rationale](#rationale)
  - [Goal: testing the requirements files and conda environment](#goal-testing-the-requirements-files-and-conda-environment)
  - [**NOT** a goal: these are not dockerfiles for production deployment](#not-a-goal-these-are-not-dockerfiles-for-production-deployment)
  - [Principle: Specify only direct dependencies, and let package manager resolve the rest](#principle-specify-only-direct-dependencies-and-let-package-manager-resolve-the-rest)
    - [Why separate direct from fully resolved dependencies?](#why-separate-direct-from-fully-resolved-dependencies)
  - [Possible use in future: CI/testing/dev in docker container](#possible-use-in-future-citestingdev-in-docker-container)
- [Contents: Three environments to verify](#contents-three-environments-to-verify)
- [Usage: 1) How to build the Docker images](#usage-1-how-to-build-the-docker-images)
  - [Example: build image for debianpython.dockerfile](#example-build-image-for-debianpythondockerfile)
  - [Example: build image for miniconda](#example-build-image-for-miniconda)
- [Usage: 2) How to run the docker container](#usage-2-how-to-run-the-docker-container)
    - [Debian-Python: Run the tests with the Docker container](#debian-python-run-the-tests-with-the-docker-container)
    - [miniconda: Run the tests with the Docker container](#miniconda-run-the-tests-with-the-docker-container)
- [Usage: 3) run a bash shell in the container for troubleshooting](#usage-3-run-a-bash-shell-in-the-container-for-troubleshooting)
  - [miniconda: Open a bash shell inside the running docker container](#miniconda-open-a-bash-shell-inside-the-running-docker-container)


## Rationale

### Goal: testing the requirements files and conda environment

First and foremost these dockerfiles are intended for testing the package installation, i.e.
do the pip requirements files and the `conda-environment.yaml` work correctly.

The idea is that if the installation succeeds and the test suite with pytest passes, then the requirements / conda env should be good enough for development.

### **NOT** a goal: these are not dockerfiles for production deployment

This **not** meant to be a production setup, as you would expect in a web application. Do not use it for that.

Moreover, in principle this is a tool for a small set of users, who should be able to set this up. But if we do need something very reproducible, we could still add that setup later.

### Principle: Specify only direct dependencies, and let package manager resolve the rest

A production setup has different demands from what we want to achieve here. For production you should have  an environment that specifies the full versions for all dependencies in order to get reproducible builds. (Possibly even with some extra verifications such as hashes to know the package is the correct one, and that it has not been tampered with, for security)

However, that is not at all what we want to achieve with the current requirements files and `conda-environment.yaml`. Here we deliberately specify only the dependencies that we depend on **directly**, and we are keeping it OS independent as well.
Then we let the package manager resolve the other so-called "transitive" dependencies.

If you happen to know the way [pip-tools](https://pip-tools.readthedocs.io/en/latest/) works, then you could compare this way of working to specifying the `requirements.in` file that pip-compile fully resolves to `requirement.txt`. We are specifying and testing the first type of file here: `requirements.in`, not the requirement.txt.

> - TODO: (1) Maybe rename the requirements files for clarity: extension `.in` instead of `.txt`
> - TODO: (2) Decided if we want tooling to resolved full and locked dependencies, and which one: pip-tools, pip-env, poetry, or another,
> - TODO: (3) Can Hatch already do the job for `todo (2)`?

#### Why separate direct from fully resolved dependencies?

This approach avoids problems when we want to upgrade our dependencies or when we want the environment to work on different operating systems. If there is no indication which are our direct dependencies, then a lot more dependencies might conflict with each other and we can not easily see which ones we can upgrade and which ones have to stay at a certain version. And that can be very cumbersome to figure out.

Moreover, on different OSs you may also require different packages because some Python packages do depend on things that are OS-dependent (They require an package, or they rely on native libraries, etc.). Troubleshooting all of this can be a lot of hassle, but we can avoid that hassle by keeping the list of direct dependencies separate from the fully resolved dependencies, that may be OS dependent.

### Possible use in future: CI/testing/dev in docker container

That said, with some work you could also use these containers for testing or even developing.
We might be able to use it for CI / Jenkins in the near future.

Further, this may be useful if you are on Windows or Mac, because the tool is currently only being used and tested on Linux and this would give you a Linux environment.

Though for Windows, working in either a conda environment, or inside WSL, are alternatives and frankly those options require less work than a Docker setup.

---

## Contents: Three environments to verify

There are three dockerfiles for three types of environments:

- [debianpython.dockerfile](debianpython.dockerfile): Uses only pip, in a Debian docker image.
- [miniconda.dockerfile](miniconda.dockerfile): Installs the packages in miniconda, starting from the [continuumio/miniconda3 image](https://hub.docker.com/r/continuumio/miniconda3).
- [miniforge.dockerfile](miniforge.dockerfile): Installs the packages in miniforge, the Open Source alternative to miniconda, and start from the [condaforge/miniforge3 docker image](https://hub.docker.com/r/condaforge/miniforge3)

---

## Usage: 1) How to build the Docker images

### Example: build image for debianpython.dockerfile

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

###  Example: build image for miniconda

The commands are very similar to the debianpython.dockerfile but with a second example we can show what you need to change when you use a different docker file or docker image. The commands for miniforge are completely analogous.

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

## Usage: 2) How to run the docker container

This runs the unit tests with pytest. If this succeeds your Docker environment should be OK.

#### Debian-Python: Run the tests with the Docker container

Example for the example above with debianpython.dockerfile, using the same image name+tag
`stac-catalog-builder-debianpython:latest`.

```bash
docker run --rm -ti stac-catalog-builder-debianpython:latest
```

Open a bash shell inside the running docker container:

```bash
docker run --rm -ti --entrypoint bash stac-catalog-builder-debianpython:latest
```

#### miniconda: Run the tests with the Docker container

```shell
docker run --rm -ti stac-catalog-builder-miniconda:latest
```

## Usage: 3) run a bash shell in the container for troubleshooting

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

Note that the the `-v ` option for the source of the bind volume has a different value in Powershell.

```powershell
docker run --rm -ti -v ${pwd}:/src --entrypoint bash stac-catalog-builder-miniconda:latest
```
