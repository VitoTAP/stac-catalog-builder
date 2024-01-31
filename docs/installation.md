
# Setup - Installation
- [Setup - Installation](#setup---installation)
  - [Python version, in a virtualenv or in a conda environment.](#python-version-in-a-virtualenv-or-in-a-conda-environment)
    - [Create Conda Environment with `conda-environment.yaml`](#create-conda-environment-with-conda-environmentyaml)
      - [Notes regarding conda environments](#notes-regarding-conda-environments)
    - [How to Manually Create the Conda Environment](#how-to-manually-create-the-conda-environment)
      - [Create the Conda Environment:](#create-the-conda-environment)
      - [Activate the Environment:](#activate-the-environment)
      - [Install Dependencies](#install-dependencies)
        - [Installation for Using the STAC Builder Tool Only, no Development](#installation-for-using-the-stac-builder-tool-only-no-development)
        - [Install for Developing the Tool: `requirements-dev.txt`](#install-for-developing-the-tool-requirements-devtxt)
  - [For developers: Updates to do when dependencies have changed:](#for-developers-updates-to-do-when-dependencies-have-changed)
  - [Export Conda Environment as `environment.yaml`](#export-conda-environment-as-environmentyaml)

## Python version, in a virtualenv or in a conda environment.

Install python 3.11 or higher via conda, pyenv or alike.

On Terrascope you will need to stick to conda because the old software there makes it complicated to install certain dependencies that a virtualenv can not install. These dependencies aren't Python packages and the OS yum repository doesn't have an up to date version of those dependencies.

Either of three methods should work:

- A) Use the `conda-environment.yaml` file to create a conda environment.
- B) Create the conda environment manually and install each dependency.
    You can follow the packages in requirements/requirements.txt.
- C) Create a virtualenv and use the requirements file to install the dependencies.

For Terrascope, using conda is the easiest way. Otherwise you will need to deal with outdated dependencies such as openssl. Some of these are not Python packages so you cannot isolate those with a virtualenv, but as long as conda has a package for that tool then you can install a newer version inside conda, independent of the operating systems's libraries.

### Create Conda Environment with `conda-environment.yaml`

TL;DR

```bash
conda env create -f conda-environment.yaml python=3.11
```

#### Notes regarding conda environments

You can use either conda or its faster drop-in replacement mamba. (mamba is available on the conda-forge channel)

Also, while most people use the Anaconda distribution, there is also a completely Free and Open Source (FOSS) alternative called [Miniforge](https://github.com/conda-forge/miniforge)

Our STAC catalog builder has actually been developed with Miniforge.

Miniforge uses the open source package repository [conda-forge](https://conda-forge.org/) by default. In Anaconda you would need to specify that channel if you install it manually, but the environment file already specifies the option to use that channeL

Unfortunately we found out that some of the versions of dependencies that we want to use are not yet available in conda, neither on the conda-forge` channel nor on the regular anaconda channel.
So for the moment you cannot use a pure conda environment, but you have to mix in some pip packages.
While this is something we normally like to avoid, at present it does not seem worth sticking to the old dependency versions. Eventually the conda-forge channel will catch up anyway.

### How to Manually Create the Conda Environment

The `conda` command itself can be rather slow, but there is a drop-in replacement called `mamba` that is available in both Anaconda and Miniforge.

#### Create the Conda Environment:

```bash
conda create --channel conda-forge --name stac-catalog-builder python=3.11
```

#### Activate the Environment:

```bash
conda activate stac-catalog-builder
```

#### Install Dependencies

> TODO: fix inconsistencies and repetitions in the instructions, are left overs from older versions of this these instructions.

If you prefer to use pip, the requirements files to pip-install them are these files:

- [`./requirements/requirements.txt`](./requirements/requirements.txt): to install only the application, without development support.
- [`./requirements/requirements-dev.txt`](./requirements/requirements-dev.txt): to install everthing for developing the application.

This method is essentially installing the same dependencies as listed in requirement.txt. This command just uses the corresponding conda command and lists the dependencies explicitly.
See [`./requirements/requirements.txt`](./requirements/requirements.txt) for the most up to date list of libraries you need to install.

```bash
# TODO: still issues installing some of the dependencies via conda, find out why
conda install stactools=0.5.* openeo=0.26 stac-validator=3.3 pystac[validation]=1.8 rasterio=1.3 shapely=2.0 pyproj=3.6 click=8.1
```

- [ ] TODO: **Verify updated documentation**: I found out that some of the dependencies are only available in pip, not in conda. So there are always a few left we need to pip-install. Are there other packages that only are available via pip/pypi?

> ### NOTE
>
> The reason why the above command did not work is that I was using the conda-forge channel (via Miniforge) and those versions are not yet available on that channel.
> Also openeo is not available on conda-forge, only on the regular conda channel.
> I will have to test this in a regular conda install.
>
> Highest versions available in conda-forge (d.d. 2024-01-09):
>
> package | version on conda-forge | desired version
> --- | --- | ---
> pystac | 1.6.1 | 1.8
> stac-validator | 3.2.0 | 3.3
> stactools | 0.4.2 | 0.5
> shapely | 1.8.5 | 2.0
> pyproj | 3.4.0 | 3.6
> click  | 8.1.3 | 8.1
> openeo | N/A on conda-forge | 0.26



##### Installation for Using the STAC Builder Tool Only, no Development

In a virtualenv, or alike:

```bash
python3 -m pip install -r requirements/requirements.txt
python3 -m pip install .
```

##### Install for Developing the Tool: `requirements-dev.txt`

```bash
python3 -m pip install -r requirements/requirements-dev.txt
python3 -m pip install -e .
```

## For developers: Updates to do when dependencies have changed:

Please update the following files:

- Update the Python requirements files, in the folder [requirements](../requirements):
- Update the conda environment file (export it again): `environment.yaml`
  - See: section [Export Conda Environment as `environment.yaml`](#export-conda-environment-as-environmentyaml)

There are three requirements files, a main one for the application itself and tw that add aditional dependencies for testing and developing:

- [requirements/requirements.txt](../requirements/requirements.txt)
  - Contains requirements for running the application.
- [requirements/requirements-test.txt](../requirements/requirements-test.txt)
  - Contains additional requirements needed for running the test suite.
  - It imports `requirements.txt`
- [requirements/requirements-dev.txt](../requirements/requirements-dev.txt)
  - Contains additional requirements needed for developing the application.
  - It imports `requirements-test.txt`


## Export Conda Environment as `environment.yaml`

When you change the dependencies

```bash
conda env export --no-builds -f conda-environment.yaml
```
