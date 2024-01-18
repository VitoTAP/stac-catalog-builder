# How-to Run the STAC Catalog Builder

### Getting Help

```bash
python3 stacbuilder --help
python3 stacbuilder build --help
```

### Building a Collection

You need to set up a configuration for the collection to generate the STAC files.
To help with this we have a template configuration, which you can find here inside the git repo:

[./configs-datasets/config-template](configs-datasets/config-template)

We also store the configuration of actual datasets, and you can also look at those to see a fully worked out example that already runs out of the box.

The configuration consists of two files:

#### 1. `config-collection.json`

See: [config-collection.json](configs-datasets/config-template/config-collection.json)

This is the actual configuration file.

It is a JSON file containing some variables that we need to know to ba able to build the collection, for example, the collection ID, title, description. et cetera.

This file is loaded and validated through Pydantic.

Below some explanation of the items in the config file.
Since the JSON format doesn't support comments, I'm commenting it here instead, in the README.

The code that defines these configurations is located in: [./stacbuilder/config.py](./stacbuilder/config.py)
Each configuration is a subclass of Pydantic BaseModel.
These models can also be nested. You will find CollectionConfig uses other models as the contents of some of its fields.


Collection configuration file: corresponds to class `CollectionConfig`.

```yaml
{
    "collection_id": "your-collection-id",
    "title": "Title for your collection",
    "description": "Description for your collection",

    // instruments is a list of strings
    "instruments": [],

    // keywords is a list of strings
    "keywords": [],

    // keywords is a list of strings
    "mission": [],

    // platform is a list of strings
    "platform": [],

    // providers is defined as a list of ProviderModels, see. the class definition: ProviderModels
    "providers": [
        {
            "name": "VITO",
            "roles": [
                "licensor",
                "processor",
                "producer"
            ],
            "url": "https://www.vito.be/"
        }
    ],

    // layout_strategy is something from pystac that automatically creates
    // subfolders for the STAC items JSON files.
    "layout_strategy_item_template": "${collection}/{year}",

    // We extract some metadata from the geotiff's file path using a subclass of InputPathParser.
    // Path parsers are defined in stacbuilder/pathparsers.py
    //
    // You probably will need to write a subclass to customize for your particular
    // needs, unless it happens to fit a simple existing case.
    //
    // Below you fill in the class name to use.
    // For how the tool finds the right class:
    //  see the classes InputPathParserFactory and InputPathParser.
    //
    // While it is technically also possible to define parameters that would be
    // passed to the constructor, at present, it is just easier to just write a
    // subclass specifically for the collection and give that a no-arguments constructor.
    "input_path_parser": {
        "classname": "RegexInputPathParser"
    },


    //
    // `item_assets` defines what assets STAC Items have and what bands the assets contain.
    //
    // This is a dictionary that maps the asset type to an asset definition
    // Asset definition are defined by the class `AssetConfig`.
    // We assume each file is an asset, but depending on the file name it could
    // be a different type of item, therefore "item_type".
    // if there is only on type of item, make your InputPathParser return a fixed value for
    "item_assets": {
        "some_item_type": {
            "title": "REPLACE_THIS--BAND_NAME",
            "description": "REPLACE_THIS--BAND_NAME",
            "eo_bands": [
                {
                    "name": "REPLACE_THIS--BAND_NAME",
                    "description": "REPLACE_THIS--BAND_DESCRIPTION",
                    "data_type": "float32",
                    "sampling": "area",
                    "spatial_resolution": 100
                }
            ]
        }
    },

    // See .stacbuilder/config.py,  class: CollectionConfig
    // This dictionary allows us to fill in or overwrite some fields with values
    // that we just want to give a fixed value. This is done at the very end of
    // the process in a post-processing step.
    "overrides": {}
}
```




#### 2. `Makefile`

See: [configs-datasets/config-template/Makefile](configs-datasets/config-template/Makefile)

The Makefile automates the CLI commands and some helper commands to copy and list files.

It is basically there to have consistent values for the parameters you pass to the stacbuilder tool.
You can also use `make --dry-run` to show you the exact command it would run if you want to see an example to run the command directly.

Make calls the python script and passes the different CLI commands and arguments using a bunch of variables for configuration.
Basically, instead of explaining bash commands that show you how to use the Python script, we give you the commands in the Makefile.

Example: dry-run

```shell
make --dry-run -f configs-datasets/PEOPLE_EA/Landsat_three-annual_NDWI_v1/Makefile build-collection
```

Output:

```shell
$ make --dry-run -f configs-datasets/PEOPLE_EA/Landsat_three-annual_NDWI_v1/Makefile build-collection
/home/demo-user/mambaforge/envs/stac-catalog-builder/bin/python3 stacbuilder \
        -v build \
        -g "*/*.tif" \
        -c /home/demo-user/stac-catalog-builder/configs-datasets/PEOPLE_EA/Landsat_three-annual_NDWI_v1/config-collection.json \
        --overwrite \
        /data/MTDA/PEOPLE_EA/Landsat_three-annual_NDWI_v1/ \
        /home/demo-user/stac-catalog-builder/configs-datasets/configs-datasets/PEOPLE_EA/Landsat_three-annual_NDWI_v1/STAC_wip/v0.4
```

I tried to make the Makefile reasonably self-documenting.

- There configuration variables are all at the top and explained with comments.
- There is a `help` make target which is also the default, so you can see what all the make target are for.

Example:

```
make help

# Or simply `make` without any targets also shows the help target.
make
```

Typically you will have to run make from the root of the git repo because there are some issues with Python finding the stacbuilder modules. (To be fixed still)

In that case you need to point `make` to the Makefile with the `-f <path to makefile>` option.

Example: `-f <path to makefile>`

```shell
# run this at the root of the git repo
make -f configs-datasets/PEOPLE_EA/Landsat_three-annual_NDWI_v1/Makefile
```

The configuration parameters are set up a the top of the Makefile and they are mainly the following items:

- input directory,
- file glob,
- output dir,
- where the collection config file is located
- and where the final result for the STAC files should be copied to, so other people can use it.

### The main variables to change in the Makefile

`STACBLD_PYTHON_BIN`

Set this to the path of your python executable so make uses the correct python.
This is not the nicest solution, but necessary for now to get things to work.

Example:

```make
STACBLD_PYTHON_BIN := /home/johan.schreurs/mambaforge/envs/stac-catalog-builder/bin/python3
```

CAVEAT: on Terrascope the command `python` is really an alias to the system's Python.
This can trip you up if you are not aware that `python` will not find your conda or virtualenv executable.
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

In the future this can be simplified and `PUBLISH_DIR` could point directly to the folder where you to put the STAC collections.
But for now I go by a more step-by-step process, at least until we have ironed out all the initial wrinkles.

The default values `PUBLISH_DIR_ROOT` and `PUBLISH_DIR` point to a folder inside the current git repo in `configs-datasets`
in order to have a setup that runs immediately.

---

## Stacbuilder commands and options

There are two ways to run it:

- A) Run the python file: __main__.py
- B) Run it as a module

To run it as a module you may need to be in the root of the git folder, or set `PYTHONPATH` to include it.
This is not a fully polished Python package yet, that could be installed.

As usual, there is a `--help` option that shows you the usage, but without any argument the program will also display the usage.

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
  test-openeo      Test STAC collection via load_stac in openEO.
  validate         Run STAC validation on the collection file.

```

The main command is off course `build`.
The other commands are meant for troubleshooting and show you what metadata or stac items the tool would generate (So these commands don't a STAC collection).

Another useful one is `test-openeo`, because for our purposes, the goal is that it works with load_stac in openEO.

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
