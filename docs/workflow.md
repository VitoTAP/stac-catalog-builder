# Workflow for Creating STAC Collections

## Create a collection from a directory containing GeoTIFFs

### Overview: Steps to Take

```mermaid
---
title: "Steps: Create collection from GeoTIFFs"
---
graph TD
    START_NODE([start]) --> copy_template["Copy configuration template"]
    copy_template --> fill_configuration["Fill in configuration"]
    fill_configuration --> test_tiffs_found["list_tiffs: test if right files are found"]
    test_tiffs_found --> check_assetmetadata["list_metadata: check AssetMetadata are correct"]
    check_assetmetadata --> check_stac_items["list_stac_items: check STAC items are correct"]
    check_stac_items --> build_collection[build_collection]
    build_collection --> END_NODE([end])
```

### Data flow

What data is produced from what, from source to end result:

```mermaid
---
title: "Data flow: Create collection from GeoTIFFs"
---
graph LR;
    geotiffs[Directory containing GeoTIFF files] --> AssetMetadata["AssetMetadata\n(in memory)"];
    AssetMetadata --> STACItems["STACItems\n(in memory)"];
    STACItems --> STACCollection["STACCollection\n(in memory)"];
    STACCollection --> collection_on_disk["STAC collection & items on disk\ncollection.json"];
```

### Pipeline components

Which components convert which input data to what output?

The AssetMetadataPipeline is a shared part of all pipelines that should be the same, no matter what the source data is, or what the destination is.

We basically plug in a component at the input side and at the output side of AssetMetadataPipeline.
That input expects a list (or any iterable) of AssetMetadata objects, that represent the file or product, AKA the asset.

At its output the AssetMetadata produces a pystac Collection, and it saves that to file, including all its STAC items, and optionally all its sub-collections, if you chose to create a group of collections.


```mermaid
---
title: "Pipeline: Create collection from GeoTIFFs"
---
graph LR;
    geotiffs[(Directory containing GeoTIFF files)] -->MapGeoTiffToAssetMetadata
    MapGeoTiffToAssetMetadata --> AssetMetadata(("AssetMetadata\n(in memory)"))
    AssetMetadata --> AssetMetadataPipeline[AssetMetadataPipeline]
    AssetMetadataPipeline --> STACCollection(("STAC Collection\n(in memory)"));
    AssetMetadataPipeline --> collection_on_disk[("STAC collection & items on disk\ncollection.json")];
```

A closer look at what happens inside the AssetMetadataPipeline.
Internally the AssetMetadataPipeline uses smaller pipeline steps to do parts of the work.
This keep the classes smaller and simpler, so they are easier to understand, and also easier to write tests for.

```mermaid
---
title: "AssetMetadataPipeline: components / steps"
---
graph TD
    AssetMetadata(("AssetMetadata\n(in memory)")) --> MapMetadataToSTACItem[MapMetadataToSTACItem]
    MapMetadataToSTACItem --> STACItems(("STACItems\n(in memory)"))
    STACItems --> STACCollectionBuilder[STACCollectionBuilder]
    STACCollectionBuilder --> STACCollection(("STAC Collection\n(in memory)"));
    STACCollection -- "AssetMetadataPipeline saves collection" --> collection_on_disk[("STAC collection & items on disk\ncollection.json")];
```



## Grouped collections

TODO: describe what a grouped collection is. Maybe we need a better name.

How to translate this to a diagram that shows what is different?
