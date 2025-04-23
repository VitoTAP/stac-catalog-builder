# STAC Catalog Builder

This tool generates a STAC collection from a set of GeoTiff images.

It is mainly intended to create STAC collections and catalogs for use in [openEO](https://openeo.org/), with the load_stac process.

It requires a some configuration for the fields we need to fill in, but the goal is to make it much easier to generate STAC collections from a set of EO images.

For now it only supports GeoTIFFs. For example, netCDF is not supported yet, because it can be a lot more complex to extract info from than GeoTIFF.
We wanted to start with GeoTIFF and we can see about other needs later.

This tool also provied support for uploading to a STAC API.

See also: [Goals and User Stories](docs/goals-and-user-stories.md): A longer explanation of the goals or use case for the STAC catalog builder.

## Documentation
**[WARNING] Do not use notebooks as these will fail**


## Getting Started

- [Setup - Installation](docs/installation.md)
- [How to Configure and Run a New Dataset](docs/how-to-configure-new-dataset.md)
- [Workflow of the Stac Builder Explained](docs/workflow.md)
- [How to run the STAC builder from command line](docs/how-to-run-stacbuilder-cli.md)

## Development

- [Developer Guidelines](docs/developer-guidelines.md)
- [Goals and User Stories](docs/goals-and-user-stories.md)
