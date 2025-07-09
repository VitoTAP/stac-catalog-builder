# How to Configura a New Dataset

You need to set up a configuration for the collection to generate the STAC files.
To help with this we have a template configuration, which you can find here inside the git repo:

[./configs-datasets/config-template](configs-datasets/config-template)

We also store the configuration of actual datasets, and you can also look at those to see a fully worked out example that already runs out of the box.

The configuration consists of two files:

#### 1. `config-collection.json`

See: [config-collection.json](configs-datasets/config-template/config-collection.json)

This is the actual configuration file.

It is a JSON file containing some variables that we need to know to ba able to build the collection, for example, the collection ID, title, description. etc.

This file is loaded and validated through Pydantic.

Below some explanation of the items in the config file.

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
    // There are several path parsers available, but you can also write your own.
    //
    // While it is technically also possible to define parameters that would be
    // passed to the constructor, at present, it is just easier to just write a
    // subclass specifically for the collection and give that a no-arguments constructor.
    //
    // Fields that typically are required are `asset_type`, and `year` and `month` (only for monthly)
    // The `asset_type` is equivalent to the band name.
    "input_path_parser": {
        "classname": "DefaultInputPathParser",
        "parameters": {
            "regex_pattern": ".*/(?P<asset_type>MIN|MAX)/(?P<year>\\d{4})-(?P<month>\\d{2}).tif$",
            "period": "monthly",
            "fixed_values": {
            }
        }
    },

    // `item_assets` defines what assets STAC Items have and what bands the assets contain.
    //
    // This is a dictionary that maps the asset type to an asset definition
    // Asset definition are defined by the class `AssetConfig`.
    //
    // We assume each file is an asset, but depending on the file name it could
    // be a different type of item, therefore "item_type".
    //
    // if there is only on type of item, you can set the `asset_type` as a fixed value
    // in the `input_path_parser`.
    // Extra fields like data_type, sampling, spatial_resolution are optional.
    "item_assets": {
        "some_item_type": {
            "title": "REPLACE_THIS--BAND_NAME",
            "description": "REPLACE_THIS--BAND_NAME",
            "eo_bands": [
                {
                    "name": "REPLACE_THIS--BAND_NAME",
                    "description": "REPLACE_THIS--BAND_DESCRIPTION",
                    "data_type": "float32", // optional
                    "sampling": "area", // optional
                    "spatial_resolution": 100 // optional
                }
            ]
        }
    },

    // See .stacbuilder/config.py,  class: CollectionConfig
}
```




#### 2. `workflow.py`

See: [configs-datasets/config-template/workflow.py](configs-datasets/config-template/workflow.py)

This is a Python file that contains the workflow to run the STAC builder.

It is advised to run this code step by step by commenting out the steps you do not want to run. This way you can validate each step before running the next one.

### Item Postprocessor
Some advanced usecases require you to write an `item_postprocessor`. This is a function that accespt a pystac item and modifies it. This is useful for example to add extra fields to the item, or to modify the asset metadata.
