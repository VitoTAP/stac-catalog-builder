"""
The main module, which is run as the program.

This defines the Command Line Interface of the application.
We want to keep this layer thin so we can write unit/integration tests for the
functionality underneath without dealing directly with the CLI.

The functions you find in this module should remain very simple.
"""

import logging
import pprint
from pathlib import Path

import click
import pydantic
import pydantic.errors

from stacbuilder import commandapi
from stacbuilder.config import CollectionConfig

_logger = logging.getLogger(__name__)


@click.group
@click.option("-v", "--verbose", is_flag=True, help="show debug output")
def cli(verbose):
    """Main CLI group. This is the base command.

    Everything that is in here will run at the start of every stacbuilder command.
    """

    #
    # Set up logging for the application.
    #
    log_level = logging.INFO
    if verbose:
        log_level = logging.DEBUG

    # create console handler with a higher log level
    console_handler = logging.StreamHandler()
    console_handler.setLevel(log_level)
    # create formatter and add it to the handlers
    formatter = logging.Formatter("%(levelname)-7s | %(asctime)s | %(message)s")
    console_handler.setFormatter(formatter)
    logging.basicConfig(handlers=[console_handler], level=log_level)


@cli.command
@click.option(
    "-g",
    "--glob",
    default="*",
    type=click.STRING,
    help="glob pattern for collecting the GeoTIFF files. Example: */*.tif",
)
@click.option(
    "-c",
    "--collection-config",
    type=click.Path(exists=True, dir_okay=False, file_okay=True),
    help="Configuration file for the collection",
)
@click.option("-m", "--max-files", type=int, default=-1, help="Stop processing after this maximum number of files.")
@click.argument(
    "inputdir",
    type=click.Path(exists=True, dir_okay=True, file_okay=False),
)
@click.argument(
    "outputdir",
    type=click.Path(dir_okay=True, file_okay=False),
)
def build(glob, collection_config, inputdir, outputdir, max_files):
    """Build a STAC collection from a directory of GeoTIFF files."""
    commandapi.build_collection(
        collection_config_path=collection_config,
        glob=glob,
        input_dir=inputdir,
        output_dir=outputdir,
        max_files=max_files,
    )


@cli.command
@click.option(
    "-g",
    "--glob",
    default="*",
    type=click.STRING,
    help="glob pattern for collecting the GeoTIFF files. Example: */*.tif",
)
@click.option(
    "-c",
    "--collection-config",
    type=click.Path(exists=True, dir_okay=False, file_okay=True),
    help="Configuration file for the collection",
)
@click.option("-m", "--max-files", type=int, default=-1, help="Stop processing after this maximum number of files.")
@click.argument(
    "inputdir",
    type=click.Path(exists=True, dir_okay=True, file_okay=False),
)
@click.argument(
    "outputdir",
    type=click.Path(dir_okay=True, file_okay=False),
)
def build_grouped_collections(glob, collection_config, max_files, inputdir, outputdir):
    """Build a STAC collection from a directory of GeoTIFF files."""
    commandapi.build_grouped_collections(
        collection_config_path=collection_config,
        glob=glob,
        input_dir=inputdir,
        output_dir=outputdir,
        max_files=max_files,
    )


@cli.command
@click.option(
    "-g", "--glob", default="*", type=click.STRING, help="glob pattern to collect the GeoTIFF files. example */*.tif"
)
@click.argument(
    "inputdir",
    type=click.Path(exists=True, dir_okay=True, file_okay=False),
)
@click.option("-m", "--max-files", type=int, default=-1, help="Stop processing after this maximum number of files.")
def list_tiffs(glob, inputdir, max_files):
    """List which GeoTIFF files will be selected with this input dir and glob pattern."""
    result = commandapi.list_input_files(glob=glob, input_dir=inputdir, max_files=max_files)
    print(f"Found {len(result)} files:")
    for file in result:
        print(file)


@cli.command
@click.option(
    "-g", "--glob", default="*", type=click.STRING, help="glob pattern to collect the GeoTIFF files. example */*.tif"
)
@click.option(
    "-c",
    "--collection-config",
    type=click.Path(exists=True, dir_okay=False, file_okay=True),
    help="Configuration file for the collection",
)
@click.option("-m", "--max-files", type=int, default=-1, help="Stop processing after this maximum number of files.")
@click.argument(
    "inputdir",
    type=click.Path(exists=True, dir_okay=True, file_okay=False),
)
def list_metadata(collection_config, glob, inputdir, max_files):
    """List intermediary asset metadata, one for each GeoTIFF.

    You can optionally save the metadata as a shapefile and geoparquet so you
    can inspect the bounding boxes as well as the data.
    """
    metadata_list = commandapi.list_asset_metadata(
        collection_config_path=collection_config,
        glob=glob,
        input_dir=inputdir,
        max_files=max_files,
    )
    if not metadata_list:
        print("No asset metadata found")
        return

    for meta in metadata_list:
        pprint.pprint(meta.to_dict(include_internal=True))
        print()
    print()


@cli.command
@click.option(
    "-g", "--glob", default="*", type=click.STRING, help="glob pattern to collect the GeoTIFF files. example */*.tif"
)
@click.option(
    "-c",
    "--collection-config",
    type=click.Path(exists=True, dir_okay=False, file_okay=True),
    help="Configuration file for the collection",
)
@click.option("-m", "--max-files", type=int, default=-1, help="Stop processing after this maximum number of files.")
@click.argument(
    "inputdir",
    type=click.Path(exists=True, dir_okay=True, file_okay=False),
)
def list_items(collection_config, glob, inputdir, max_files):
    """List generated STAC items.

    You can optionally save the metadata as a shapefile and geoparquet so you
    can inspect the bounding boxes as well as the data.
    """
    stac_items, failed_files = commandapi.list_stac_items(
        collection_config_path=collection_config,
        glob=glob,
        input_dir=inputdir,
        max_files=max_files,
    )
    if not stac_items:
        print("No STAC items were generated")

    for item in stac_items:
        pprint.pprint(item.to_dict())
    for file in failed_files:
        print(f"Item could not be generated for file: {file}")


@cli.command
@click.argument("collection_file", type=click.Path(exists=True, dir_okay=False, file_okay=True))
def show_collection(collection_file):
    """Read the STAC collection file and display its contents.

    You can use this to see if it can be loaded.
    """
    collection = commandapi.load_collection(collection_file)
    pprint.pprint(collection.to_dict(), indent=2)


@cli.command
@click.argument("collection_file", type=click.Path(exists=True, dir_okay=False, file_okay=True))
def validate(collection_file):
    """Run STAC validation on the collection file."""
    commandapi.validate_collection(collection_file)


#
# Subcommands for working with the collection configuration file.
# Mostly to validate and troubleshoot the configuration.
#


@cli.group
def config():
    """Subcommands for collection configuration."""
    pass


@config.command
def schema():
    """Show the JSON schema for CollectionConfig files or objects."""
    schema = CollectionConfig.model_json_schema()
    click.echo(pprint.pformat(schema, indent=2))


@config.command
@click.argument("config_file", type=click.Path(exists=True, dir_okay=False, file_okay=True))
def validate_config(config_file):
    """Check whether a collection configuration file is in the correct format.
    This only checks if the format is valid. It can not check whether the contents make sense.
    """
    config_file = Path(config_file)
    if not config_file.exists():
        raise FileNotFoundError(f"config_file could not be found. {config_file=}")

    try:
        CollectionConfig.from_json_file(config_file)
    except pydantic.ValidationError as exc:
        click.echo(click.style("ERROR: NOT VALID: \n" + str(exc), fg="red"))
    else:
        click.echo(click.style("OK: is valid configuration file", fg="green"))


@config.command
@click.argument("config_file", type=click.Path(exists=True, dir_okay=False, file_okay=True))
def show_config(config_file):
    """Read the config file and show how the parsed contents.

    This is a way to check if you are getting what you expected from the configuration.
    This may be important for things like asset and band names, titles and descriptions.
    """
    config_file = Path(config_file)
    if not config_file.exists():
        raise FileNotFoundError(f"config_file could not be found. {config_file=}")

    try:
        configuration = CollectionConfig.from_json_file(config_file)
    except pydantic.ValidationError as exc:
        click.echo(click.style("ERROR: NOT VALID: \n" + str(exc), fg="red"))
    else:
        pprint.pprint(configuration.model_dump(), indent=2, width=160)


@config.command
def docs():
    click.echo(CollectionConfig.__doc__)

    schema = CollectionConfig.model_json_schema()
    click.echo(pprint.pformat(schema, indent=2))

    def show_descriptions(schema_dict, path=None):
        for key, value in schema_dict.items():
            if not path:
                path = [key]

            if key == "description":
                print(path, value)
            if isinstance(value, dict):
                path.append(key)
                show_descriptions(value, path)

    show_descriptions(schema)


if __name__ == "__main__":
    cli()
