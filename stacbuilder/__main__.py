import json
import logging
import pprint
from pathlib import Path


import click
import pydantic
import pydantic.errors


from openeo.util import rfc3339


from stacbuilder.builder import (
    command_load_collection,
    command_validate_collection,
    command_post_process_collection,
    CommandsNewPipeline,
)
from stacbuilder.config import CollectionConfig
from stacbuilder.verify_openeo import verify_in_openeo


_logger = logging.getLogger(__name__)


@click.group()
@click.option("-v", "--verbose", is_flag=True, help="show debug output")
def cli(verbose):
    log_level = logging.INFO
    if verbose:
        log_level = logging.DEBUG

    _logger.setLevel(log_level)
    # create console handler with a higher log level
    ch = logging.StreamHandler()
    ch.setLevel(log_level)
    # create formatter and add it to the handlers
    formatter = logging.Formatter("%(levelname)-7s | %(message)s")
    ch.setFormatter(formatter)
    # add the handlers to the logger
    _logger.addHandler(ch)


@cli.command()
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
@click.option("--overwrite", is_flag=True, help="Replace the entire output directory when it already exists")
@click.option("-m", "--max-files", type=int, default=-1, help="Stop processing after this maximum number of files.")
@click.argument(
    "inputdir",
    type=click.Path(exists=True, dir_okay=True, file_okay=False),
)
@click.argument(
    "outputdir",
    type=click.Path(dir_okay=True, file_okay=False),
)
def build_collection_old_pipeline(glob, collection_config, overwrite, inputdir, outputdir, max_files):
    """Build a STAC collection from a directory of GeoTIFF files."""
    from stacbuilder.builder import old_command_build_collection

    print(
        'WARNING: this command is deprecated and will only be kept for a while for back-testing. Use "build" instead.'
    )

    old_command_build_collection(
        collection_config_path=collection_config,
        glob=glob,
        input_dir=inputdir,
        output_dir=outputdir,
        overwrite=overwrite,
        max_files=max_files,
    )


@cli.command()
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
@click.option("--overwrite", is_flag=True, help="Replace the entire output directory when it already exists")
@click.option("-m", "--max-files", type=int, default=-1, help="Stop processing after this maximum number of files.")
@click.argument(
    "inputdir",
    type=click.Path(exists=True, dir_okay=True, file_okay=False),
)
@click.argument(
    "outputdir",
    type=click.Path(dir_okay=True, file_okay=False),
)
def build(glob, collection_config, overwrite, inputdir, outputdir, max_files):
    """Build a STAC collection from a directory of GeoTIFF files."""
    CommandsNewPipeline.build_collection(
        collection_config_path=collection_config,
        glob=glob,
        input_dir=inputdir,
        output_dir=outputdir,
        overwrite=overwrite,
        max_files=max_files,
    )


@cli.command()
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
@click.option("--overwrite", is_flag=True, help="Replace the entire output directory when it already exists")
@click.option("-m", "--max-files", type=int, default=-1, help="Stop processing after this maximum number of files.")
@click.argument(
    "inputdir",
    type=click.Path(exists=True, dir_okay=True, file_okay=False),
)
@click.argument(
    "outputdir",
    type=click.Path(dir_okay=True, file_okay=False),
)
def build_grouped_collections(glob, collection_config, overwrite, inputdir, outputdir, max_files):
    """Build a STAC collection from a directory of GeoTIFF files."""
    CommandsNewPipeline.build_grouped_collections(
        collection_config_path=collection_config,
        glob=glob,
        input_dir=inputdir,
        output_dir=outputdir,
        overwrite=overwrite,
        max_files=max_files,
    )


@cli.command()
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
    CommandsNewPipeline.list_input_files(glob=glob, input_dir=inputdir, max_files=max_files)


@cli.command()
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
@click.option("-s", "--save-dataframe", is_flag=True, help="Also save the data to shapefile and geoparquet.")
@click.argument(
    "inputdir",
    type=click.Path(exists=True, dir_okay=True, file_okay=False),
)
def list_metadata(collection_config, glob, inputdir, max_files, save_dataframe):
    """List intermediary metadata per GeoTIFFs.

    You can optionally save the metadata as a shapefile and geoparquet so you
    can inspect the bounding boxes as well as the data.
    """
    CommandsNewPipeline.list_metadata(
        collection_config_path=collection_config,
        glob=glob,
        input_dir=inputdir,
        max_files=max_files,
        save_dataframe=save_dataframe,
    )


@cli.command()
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
@click.option("-s", "--save-dataframe", is_flag=True, help="Also save the data to shapefile and geoparquet.")
@click.argument(
    "inputdir",
    type=click.Path(exists=True, dir_okay=True, file_okay=False),
)
def list_items(collection_config, glob, inputdir, max_files, save_dataframe):
    """List generated STAC items.

    You can optionally save the metadata as a shapefile and geoparquet so you
    can inspect the bounding boxes as well as the data.
    """
    CommandsNewPipeline.list_stac_items(
        collection_config_path=collection_config,
        glob=glob,
        input_dir=inputdir,
        max_files=max_files,
        save_dataframe=save_dataframe,
    )


@cli.command()
@click.argument("collection_file", type=click.Path(exists=True, dir_okay=False, file_okay=True))
def show_collection(collection_file):
    """Read the STAC collection file and display its contents.

    You can use this to see if it can be loaded.
    """
    command_load_collection(collection_file)


@cli.command()
@click.argument("collection_file", type=click.Path(exists=True, dir_okay=False, file_okay=True))
def validate(collection_file):
    """Run STAC validation on the collection file."""

    command_validate_collection(collection_file)


@cli.command()
@click.option(
    "-o",
    "--outputdir",
    required=False,
    type=click.Path(dir_okay=True, file_okay=False),
)
@click.option(
    "-c",
    "--collection-config",
    type=click.Path(exists=True, dir_okay=False, file_okay=True),
    help="Configuration file for the collection",
)
@click.argument("collection_file", type=click.Path(exists=True, dir_okay=False, file_okay=True))
def post_process(outputdir, collection_config, collection_file):
    """Run only the postprocessing.

    Optionally saves the postprocessing result as a separate collection so you
    can re-run easily.
    You make have to do that many times when debugging postpreocessing
    and waiting for collections to be build is annoying.
    """
    command_post_process_collection(
        collection_file=collection_file, collection_config_path=collection_config, output_dir=outputdir
    )


@cli.command
@click.option("-b", "--backend-url", type=click.STRING, help="URL for open-EO backend", default="openeo-dev.vito.be")
@click.option(
    "-o",
    "--out-dir",
    type=click.Path(exists=False, dir_okay=True, file_okay=False),
    help="Directory to save batch jobs outputs (GTIFF)",
    default=".",
)
@click.option("--bbox", type=click.STRING, default="", help="bounding box")
@click.option("-e", "--epsg", type=int, help="CRS of bbox as an EPSG code")
@click.option(
    "-m", "--max-extent-size", type=float, default=0.1, help="Maximum size of the spatial extent (in degrees)"
)
@click.option("--start-dt", type=click.STRING, help="Start date+time of the temporal extent")
@click.option("--end-dt", type=click.STRING, help="End date+time of the temporal extent")
@click.option("-n", "--dry-run", is_flag=True, help="Do a dry-run, don't execute the batch job")
@click.option("-v", "--verbose", is_flag=True, help="Make output more verbose")
@click.argument(
    "collection_file",
    type=click.Path(exists=True, dir_okay=False, file_okay=True),
)
def test_openeo(backend_url, out_dir, collection_file, bbox, epsg, max_extent_size, start_dt, end_dt, dry_run, verbose):
    """Test STAC collection via load_stac in open-EO.

    It guesses a reasonable spatial and temporal extent based on what
    extent the collection declares.
    """
    if bbox:
        bbox = json.loads(bbox)

    start_datetime = rfc3339.parse_datetime(start_dt) if start_dt else None
    end_datetime = rfc3339.parse_datetime(end_dt) if end_dt else None

    verify_in_openeo(
        backend_url=backend_url,
        collection_path=collection_file,
        output_dir=out_dir,
        bbox=bbox,
        epsg=epsg,
        max_spatial_ext_size=max_extent_size,
        start_datetime=start_datetime,
        end_datetime=end_datetime,
        dry_run=dry_run,
        verbose=verbose,
    )


#
# Subcommands for working with the collection configuration file.
# Mostly to validate and troubleshoot the configuration.
#


@cli.group()
def config():
    """Subcommands for collection configuration."""
    pass


@config.command()
def schema():
    """Show the JSON schema for CollectionConfig files or objects."""
    schema = CollectionConfig.model_json_schema()
    click.echo(pprint.pformat(schema, indent=2))


@config.command()
@click.argument("config_file", type=click.Path(exists=True, dir_okay=False, file_okay=True))
def validate_config(config_file):
    """Check whether a collection configuration file is in the correct format.
    This only checks if the format is valid. It can not check whether the contents make sense.
    """
    config_file = Path(config_file)
    if not config_file.exists():
        raise FileNotFoundError(f'Argument "config_file" does not exist. {config_file=}')

    try:
        CollectionConfig.from_json_file(config_file)
    except pydantic.ValidationError as exc:
        click.echo(click.style("ERROR: NOT VALID: \n" + str(exc), fg="red"))
    else:
        click.echo(click.style("OK: is valid configuration file", fg="green"))


@config.command()
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
