import json
import logging

import click


from stacbuilder.builder import (
    command_build_collection,
    command_load_collection,
    command_validate_collection,
    command_post_process_collection,
    CommandsNewPipeline,
)

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
)
@click.argument(
    "outputdir",
    type=click.Path(dir_okay=True, file_okay=False),
)
def build(glob, collection_config, overwrite, inputdir, outputdir, max_files):
    """Build a STAC collection from a directory of geotiff files."""
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
)
@click.argument(
    "outputdir",
    type=click.Path(dir_okay=True, file_okay=False),
)
def build_newpipe(glob, collection_config, overwrite, inputdir, outputdir, max_files):
    """Build a STAC collection from a directory of geotiff files."""
    CommandsNewPipeline.command_build_collection_newpipe(
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
)
@click.option("-m", "--max-files", type=int, default=-1, help="Stop processing after this maximum number of files.")
def list_tiffs(glob, inputdir, max_files):
    """List which geotiff files will be selected with this input dir and glob pattern."""
    CommandsNewPipeline.command_list_input_files(glob=glob, input_dir=inputdir, max_files=max_files)


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
    CommandsNewPipeline.command_list_metadata(
        collection_config_path=collection_config,
        glob=glob,
        input_dir=inputdir,
        max_files=max_files,
        save_dataframe=save_dataframe,
    )


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
    CommandsNewPipeline.command_list_stac_items(
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
@click.option("-n", "--dry-run", is_flag=True, help="Do a dry-run, don't execute the batch job")
@click.option("-v", "--verbose", is_flag=True, help="Make output more verbose")
@click.argument(
    "collection_file",
    type=click.Path(exists=True, dir_okay=False, file_okay=True),
)
def test_openeo(backend_url, out_dir, collection_file, bbox, epsg, max_extent_size, dry_run, verbose):
    """Test STAC collection via load_stac in open-EO.

    It guesses a reasonable spatial and temporal extent based on what
    extent the collection declares.
    """
    if bbox:
        bbox = json.loads(bbox)

    verify_in_openeo(
        backend_url=backend_url,
        collection_path=collection_file,
        output_dir=out_dir,
        bbox=bbox,
        epsg=epsg,
        max_spatial_ext_size=max_extent_size,
        dry_run=dry_run,
        verbose=verbose,
    )


if __name__ == "__main__":
    cli()
