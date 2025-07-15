# How-to Run the STAC Catalog Builder from the Command Line

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
$ python -m stacbuilder
Usage: python -m stacbuilder [OPTIONS] COMMAND [ARGS]...

  Main CLI group. This is the base command.

  Everything that is in here will run at the start of every stacbuilder
  command.

Options:
  -v, --verbose  show debug output
  --help         Show this message and exit.

Commands:
  build                           Build a STAC collection from a...
  build-grouped-collections       Build a STAC collection from a...
  config                          Subcommands for collection...
  extract-item-bboxes             Extract and save the bounding boxes...
  list-items                      List generated STAC items.
  list-metadata                   List intermediary asset metadata,...
  list-tiffs                      List which GeoTIFF files will be...
  post-process                    Run only the postprocessing.
  show-collection                 Read the STAC collection file and...
  validate                        Run STAC validation on the...
  vpp-build                       Build a STAC collection for one of...
  vpp-count-products
  vpp-count-products-per-query-slot
  vpp-get-collection-config       Display the CollectionConfig for the...
  vpp-list-items                  Show the STAC items that are...
  vpp-list-metadata               Show the AssetMetadata objects that...
  vpp-list-tcc-collections
  vpp-show-all-collection-configs
                                  Display the CollectionConfig for...
  vpp-show-stac-api-config        Show the configuration to upload to...
  vpp-upload                      Upload a collection to the STAC API.
  vpp-upload-items                Upload a STAC Items to the STAC API.
```

The main command is of course `build`.
The other commands are meant for troubleshooting and show you what metadata or stac items the tool would generate (So these commands don't create a STAC collection).

```shell
$python3 -m  stacbuilder build --help
Usage: python -m stacbuilder build [OPTIONS] INPUTDIR OUTPUTDIR

  Build a STAC collection from a directory of geotiff files.

Options:
  -g, --glob TEXT               glob pattern for collecting the geotiff files.
                                Example: */*.tif
  -c, --collection-config FILE  Configuration file for the collection
  -m, --max-files INTEGER       Stop processing after this maximum number of
                                files.
  --help                        Show this message and exit.
```
