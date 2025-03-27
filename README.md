# STAC Catalog Builder

This tool generates a STAC collection from a set of GeoTiff images.

It is mainly intended to create STAC collections and catalogs for use in [openEO](https://openeo.org/), with the load_stac process.

It requires a some configuration for the fields we need to fill in, but the goal is to make it much easier to generate STAC collections from a set of EO images.

For now it only supports GeoTIFFs. For example, netCDF is not supported yet, because it can be a lot more complex to extract info from than GeoTIFF.
We wanted to start with GeoTIFF and we can see about other needs later.

Support for uploading to a STAC API is in the making.

See also: [Goals and User Stories](docs/goals-and-user-stories.md): A longer explanation of the goals or use case for the STAC catalog builder.

## Documentation
[WARNING] Do not use notebooks as these will fail
Documentation can be found in here [docs/index.md](./docs/index.md)

Below, als the links to the important topics, for convenience:

Getting Started:

- [Installation](docs/installation.md)
- [Running the Stacbuilder Tool](docs/how-to-run-stacbuilder.md)
- [How to Configure a New Dataset](docs/how-to-configure-new-dataset.md)

Development:

- [Software Development Guidelines](docs/developer-guidelines.md)
