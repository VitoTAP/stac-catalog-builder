# STAC Catalog Builder

This tool generates a STAC collection from a set of GeoTiff images.

It requires a some configuration for the fields we need to fill in, but the goal is to make it much easier to generate STAC collections from a set of EO images.

For not it only supports GeoTIFFs. For now, netCDF is not supported yet, because it can be a lot more complex to extract info from than GeoTIFF.
We wanted to start with  GeoTIFF and we can see about other needs later.

## Setup - Installation

Install python 3.11 or higher via conda, pyenv or alike.
On Terrascope you will need to stick to conda because the old software there makes it complicated to install certain dependencies that a virtualenv can not install because they are not Python packages and the OS yum repository doesn't have an up to date version.

Either of three methods should work:

a. Use the `environment.yaml` file to create a conda environment.
    - [ ] TODO: add the `environment.yaml`, this README was copied from previous custom tool but I haven't generated it yet.
b. Create the conda environment manually and install each dependency.
    You can follow the packages in requirements/requirements.txt. 
c. Create a virtualenv and use the requirements file to install the dependencies.

For Terrascope, conda is the easiest way. Otherwise you will need to deal with outdated dependencies such as openssl. Some of these are not Python packages so you cannot isolate those with a virtualenv, but in a conda env you can often install a newer version independent of the operating systems's libraries.

- [ ] TODO: Added a Makefile recently to make installation and testing easier: Document how to use this.

### Create Conda Environment with `environment.yaml`

You can use either conda or its faster drop-in replacement mamba.
This tool has actually been developed with the FOSS alternative to Anaconda, called [Miniforge](https://github.com/conda-forge/miniforge)

Miniforge uses the open source package repository [conda-forge](https://conda-forge.org/) by default. In Anaconda you would need to specify that channel if you install it manually, but the environment file already specifies the option to use that channeL

> TODO: Export env to environment.yaml file
> TODO: There are still some problems to create an environment purely with conda (no pip). Find out why

```bash
conda env create -f environment.yaml
```

### Manually Create a Conda Environment

The `conda` command itself can be rather slow, but there is a drop-in replacement called `mamba` that is available in both Anaconda and Miniforge.


#### Create the conda environment:

```bash
conda create --name stac-catalog-builder python=3.11
```


#### Activate the environment:

```bash
conda activate stac-catalog-builder
```

#### Install dependencies

If you prefer to use pip, the dependencies are also listed in ./requirements.txt

This method is essentially installing the same dependencies as listed in requirement.txt, which you would use for a pip install. This command just uses the corresponding conda command and lists the dependencies explicitly.

```bash
# TODO: DOES NOT WORK (on Terrascope), find out why
conda install stactools=0.5.* openeo=0.23 stac-validator=3.3 pystac[validation]=1.8 rasterio=1.3 shapely=2.0 pyproj=3.6 click=8.1
```

##### Install for using the tool but no development

In a virtualenv, or alike:

```bash
python3 -m pip install -r requirements/requirements.txt
python3 -m pip install .
```

##### Install for developing the tool: requirements-dev.txt

```bash
python3 -m pip install -r requirements/requirements-dev.txt
python3 -m pip install -e .
```

## Export conda environment as environment.yaml

```bash
conda env export -f environment.yaml
```

## Running the tool

### Getting help

```bash
python3 stacbuilder --help
python3 stacbuilder build --help
```

### Building a collection

- [ ] TODO: document how to build a collection (currently in flux)