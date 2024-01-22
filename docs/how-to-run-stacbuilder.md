# How-to Run the STAC Catalog Builder

## Getting Help

```bash
python3 stacbuilder --help
python3 stacbuilder build --help
```

## Configuring a New Collection

The configuration is described in a separate document.

See: [How to Configure a New Dataset](how-to-configure-new-dataset.md)

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
