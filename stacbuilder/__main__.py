import logging

import click

from stacbuilder.builder import (
    command_build_collection,
    command_list_input_files,
    command_list_metadata,
    command_list_stac_items,
)


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
    help="glob pattern for collecting the geotiff files. Example: */*.tif",
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
    # help="Directory where the input geotiff files are stored"
)
@click.argument(
    "outputdir",
    type=click.Path(dir_okay=True, file_okay=False),
    # help="Where the save the output STAC files (directory)"
)
def build(glob, collection_config, overwrite, inputdir, outputdir, max_files):
    """Build a STAC collection from a directory of geotiff files."""
    click.echo("build")

    command_build_collection(
        collection_config_path=collection_config,
        glob=glob,
        input_dir=inputdir,
        output_dir=outputdir,
        overwrite=overwrite,
        max_files=max_files,
    )


@cli.command()
@click.option(
    "-g", "--glob", default="*", type=click.STRING, help="glob pattern to collect the geotiff files. example */*.tif"
)
@click.argument(
    "inputdir",
    type=click.Path(exists=True, dir_okay=True, file_okay=False),
    # help="Directory where the input geotiff files are stored"
)
def list_tiffs(glob, inputdir):
    """List which geotiff files will be selected with this input dir and glob pattern."""
    command_list_input_files(glob=glob, input_dir=inputdir)


@cli.command()
@click.option(
    "-g", "--glob", default="*", type=click.STRING, help="glob pattern to collect the geotiff files. example */*.tif"
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
    # help="Directory where the input geotiff files are stored"
)
def list_metadata(collection_config, glob, inputdir, max_files):
    """Build a STAC collection from a directory of geotiff files."""
    command_list_metadata(collection_config_path=collection_config, glob=glob, input_dir=inputdir, max_files=max_files)


@cli.command()
@click.option(
    "-g", "--glob", default="*", type=click.STRING, help="glob pattern to collect the geotiff files. example */*.tif"
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
    # help="Directory where the input geotiff files are stored"
)
def list_items(collection_config, glob, inputdir, max_files):
    """Build a STAC collection from a directory of geotiff files."""
    command_list_stac_items(
        collection_config_path=collection_config, glob=glob, input_dir=inputdir, max_files=max_files
    )


@cli.command()
@click.argument("collection_file", type=click.Path(exists=True, dir_okay=False, file_okay=True))
def show_collection(collection_file):
    from stacbuilder.builder import command_load_collection

    command_load_collection(collection_file)


if __name__ == "__main__":
    cli()
