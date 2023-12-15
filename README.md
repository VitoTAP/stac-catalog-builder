# STAC Catalog Builder

This tool generates a STAC collection from a set of GeoTiff images.

It requires a some configuration for the fields we need to fill in, but the goal is to make it much easier to generate STAC collections from a set of EO images.

For not it only supports GeoTIFFs. For now, netCDF is not supported yet, because it can be a lot more complex to extract info from than GeoTIFF.
We wanted to start with  GeoTIFF and we can see about other needs later.

## Setup - Installation

Install python 3.11 or higher via conda, pyenv or alike.
On Terrascope you will need to stick to conda because the old software there makes it complicated to install certain dependencies that a virtualenv can not install because they are not Python packages and the OS yum repository doesn't have an up to date version.

Either of three methods should work:

a. Use the `conda-environment.yaml` file to create a conda environment.
b. Create the conda environment manually and install each dependency.
    You can follow the packages in requirements/requirements.txt. 
c. Create a virtualenv and use the requirements file to install the dependencies.

For Terrascope, conda is the easiest way. Otherwise you will need to deal with outdated dependencies such as openssl. Some of these are not Python packages so you cannot isolate those with a virtualenv, but in a conda env you can often install a newer version independent of the operating systems's libraries.

- [ ] TODO: Added a Makefile recently to make installation and testing easier: Document how to use this.

### Create Conda Environment with `conda-environment.yaml`

You can use either conda or its faster drop-in replacement mamba.
This tool has actually been developed with the FOSS alternative to Anaconda, called [Miniforge](https://github.com/conda-forge/miniforge)

Miniforge uses the open source package repository [conda-forge](https://conda-forge.org/) by default. In Anaconda you would need to specify that channel if you install it manually, but the environment file already specifies the option to use that channeL


> TODO: There are still some problems to create an environment purely with conda (no pip). Find out why

```bash
conda env create -f environment.yaml
```

### How to Manually Create the Conda Environment

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

If you prefer to use pip, the requirements files to pip-install them are these files:

- [`./requirements/requirements.txt`](./requirements/requirements.txt): to install only the application, without development support.
- [`./requirements/requirements-dev.txt`](./requirements/requirements-dev.txt): to install everthing for developing the application.

This method is essentially installing the same dependencies as listed in requirement.txt. This command just uses the corresponding conda command and lists the dependencies explicitly.
See [`./requirements/requirements.txt`](./requirements/requirements.txt) for the most up to date list of libraries you need to install.

```bash
# TODO: DOES NOT WORK (on Terrascope), find out why
conda install stactools=0.5.* openeo=0.23 stac-validator=3.3 pystac[validation]=1.8 rasterio=1.3 shapely=2.0 pyproj=3.6 click=8.1
```

- [ ] TODO: **update documentation**: I found out that some of the dependencies these are only available in pip, not in conda. So there are always a few left we need to pip-install.


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

You need to set up a configuration for the collection to generate the STAC files.
To help with this we have a template configuration, which you can find here inside the git repo:

[./configs-datasets/config-template](configs-datasets/config-template)

We also store the configuration of actual datasets, and you can also look at those to see a fully worked out example that already runs out of the box.

The configuration consists of two files:

#### 1. `config-collection.json`

See: [config-collection.json](configs-datasets/config-template/config-collection.json)

This is the actual configuration file. 

It is a JSON file with some variables we need to know for the collection such as the collection ID, title, description ...)

This file is loaded and validated through Pydantic.

#### 2. `Makefile`

See: [configs-datasets/config-template/Makefile](configs-datasets/config-template/Makefile)

The Makefile automates the CLI commands and some helper commands to copy and list files.

It is basically there to have consistent values for the parameters you pass to the stacbuilder tool.
You can also use `make --dry-run` to show you the exact command it would run if you want to see an example and run things directly.

Example:

```shell
make --dry-run -f configs-datasets/PEOPLE_EA/Landsat_three-annual_NDWI_v1/Makefile build-collection
```

Output:

```shell
$ make --dry-run -f configs-datasets/PEOPLE_EA/Landsat_three-annual_NDWI_v1/Makefile build-collection
/home/johan.schreurs/mambaforge/envs/mf_stac-catalog-builder/bin/python3 stacbuilder \
        -v build \
        -g "*/*.tif" \
        -c /data/users/Public/johan.schreurs/codebases/vito-git-repo/STAC-catalog-builder/configs-datasets/PEOPLE_EA/Landsat_three-annual_NDWI_v1/config-collection.json \
        --overwrite \
        /data/MTDA/PEOPLE_EA/Landsat_three-annual_NDWI_v1/ \
        /data/users/Public/johan.schreurs/codebases/vito-git-repo/STAC-catalog-builder/configs-datasets/PEOPLE_EA/Landsat_three-annual_NDWI_v1/STAC_wip/v0.4
```

I tried to make the Makefile reasonably self-documenting.

- There configuration variables are all at the top and explained with comments.
- There is a `help` make target which is also the default, so you can see what all the make target are for.

Example
```
make help

# or simply, make without any targets, also shows the help target.
make
```

Typically you will have to run make from the root of the git repo because there are some issues with Python finding the stacbuilder modules. (To be fixed still)

In that case you need to point `make` to the Makefile with the `-f <path to makefile>` option.

Example:

```shell
# run this at the root of the git repo 
make -f configs-datasets/PEOPLE_EA/Landsat_three-annual_NDWI_v1/Makefile
```

The parameters are mainly:

- input directoty,
- file glob,
- output dir,
- where the collection config file is

The
- and where the final result for the STAC files should be copied so other people can use it.
  - It calls the python script and passes the different CLI commands using a bunch of variables for configuration. Basically, instead of explaining bash commands that show how to use the Python script, we give you the commands in the Makefike

The makefile contains some configuration variables at the top.

### The main variables to change in the Makefile

`STACBLD_PYTHON_BIN`

Set this to the path of your python executable so make uses the correct python

Example: 

```make
STACBLD_PYTHON_BIN := /home/johan.schreurs/mambaforge/envs/mf_stac-catalog-builder/bin/python3
```

CAVEAT: on Terrascope the comman `python` is really an alias to the systems python which is very annoying.
Always use something more specific such as the absolute path or `python3`, `python3.11`, or alike.


`DATASET_NAME`: Just the name of you collection.

Example: 

```make
DATASET_NAME :=  Landsat_three-annual_NDWI_v1
```


`TIFF_INPUT_DIR`: Directory containing the GeoTIFF input files.

Example:

```make
TIFF_INPUT_DIR := /data/MTDA/PEOPLE_EA/Landsat_three-annual_NDWI_v1/
```

`GLOB_PATTERN`:  Glob pattern to find the GeoTIFF files inside TIFF_INPUT_DIR.

```make
GLOB_PATTERN := */*.tif
```

`OUT_DIR_ROOT`: Base/root directort for the test output.

This is where you test and review the "work-in-progress" STAC files, so this is not the PUBLISH_DIR

```make
OUT_DIR_ROOT := $(WORKSPACE)/${DATASET_NAME}
```

PUBLISH_DIR_ROOT: the root folder where you share or "publish" the result with other users.
PUBLISH_DIR is then a subfolder inside ${PUBLISH_DIR_ROOT} with a subfolder for the specific dataset and its catalog version.µ

```make
PUBLISH_DIR_ROOT := /data/users/Public/johan.schreurs/PEOPLE_EA/STAC-for-review
PUBLISH_DIR := $(PUBLISH_DIR_ROOT)/${DATASET_NAME}/$(CATALOG_VERSION)
```

In the future this can be simplified and PUBLISH_DIR could point directly to the folder on /data/MTDA where you to put the STAC collections.
But for now I go by a more step-by-step process, at least until we have ironed out all the initial wrinkels.

Also I have a bunch of STAC collections that in one folder, hence the subdirectory structure.

The default values PUBLISH_DIR_ROOT and PUBLISH_DIR point to a folder inside this git repo in `configs-datasets`
in order to have a setup that runs immediately. Otherwise they would tend to user folders that may not exist in the future.


---

## Stacbuilder commands and options

There are two ways to run it: 

a) run the python file (__main__.py) 
b) or run it as a module.

To run it as a module you may need to be in the root of the git folder, or set PYTHONPATH to include it.
This is not a fully polished python package yet, that could be installed.

As usual, there is a `--help` option that shows you the usage, but without any argument the programm will aslo display the usage.

### a) Run the __main__.py file

```bash
# show the usage
python3 stacbuilder/__main__.py --help
```

### a) Run it as a module

```bash
# show the usage
python3 -m stacbuilder --help
```

### Usage

```
$ python3 -m  stacbuilder
Usage: python -m stacbuilder [OPTIONS] COMMAND [ARGS]...

Options:
  -v, --verbose  show debug output
  --help         Show this message and exit.

Commands:
  build            Build a STAC collection from a directory of geotiff...
  list-items       List generated STAC items.
  list-metadata    List intermediary metadata per GeoTIFFs.
  list-tiffs       List which geotiff files will be selected with this...
  post-process     Run only the postprocessing.
  show-collection  Read the STAC collection file and display its contents.
  test-openeo      Test STAC collection via load_stac in open-EO.
  validate         Run STAC validation on the collection file.

```

The main command is off course `build`. 
The other commands are meant for troubleshooting and show you what metadata or stac items if could generate.

Another useful one is `test-openeo` because the ultimate test is that it works with load_stac in open-EO.

```shell
$python3 -m  stacbuilder build --help
Usage: python -m stacbuilder build [OPTIONS] INPUTDIR OUTPUTDIR

  Build a STAC collection from a directory of geotiff files.

Options:
  -g, --glob TEXT               glob pattern for collecting the geotiff files.
                                Example: */*.tif
  -c, --collection-config FILE  Configuration file for the collection
  --overwrite                   Replace the entire output directory when it
                                already exists
  -m, --max-files INTEGER       Stop processing after this maximum number of
                                files.
  --help                        Show this message and exit.
```