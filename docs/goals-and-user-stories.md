# User Stories: what problems does the STAC catalog builder solve?

- [User Stories: what problems does the STAC catalog builder solve?](#user-stories-what-problems-does-the-stac-catalog-builder-solve)
  - [Introduction](#introduction)
  - [Intended users for this tool](#intended-users-for-this-tool)
    - [Summarized: how technical can the tool be?](#summarized-how-technical-can-the-tool-be)
  - [1. General goal](#1-general-goal)
  - [2. Find All Input Files via Globbing](#2-find-all-input-files-via-globbing)
    - [2.1 Globbing: Including Filee](#21-globbing-including-filee)
    - [2.2 Globbing: Excluding Files](#22-globbing-excluding-files)
  - [3 Reusing metadata in source images where possible](#3-reusing-metadata-in-source-images-where-possible)
  - [4 metadata configuration](#4-metadata-configuration)
    - [4.1 metadata configuration: options for missing metadata / metadata that can not be read or derived.](#41-metadata-configuration-options-for-missing-metadata--metadata-that-can-not-be-read-or-derived)
    - [4.2 metadata configuration: keep configuration relatively easy, low amount of code.](#42-metadata-configuration-keep-configuration-relatively-easy-low-amount-of-code)
    - [4.3 metadata configuration, "directory layout": group STAC items files together](#43-metadata-configuration-directory-layout-group-stac-items-files-together)
  - [5 Group assets that belong together into one STAC Item](#5-group-assets-that-belong-together-into-one-stac-item)
  - [6 Support other sources of data and metadata than GeoTIFF](#6-support-other-sources-of-data-and-metadata-than-geotiff)
  - [Split up very larger static STAC collections to prevent long loading times.](#split-up-very-larger-static-stac-collections-to-prevent-long-loading-times)


## Introduction

This page serves as a brief overview what the goals are for this tool.

The format is just very basic user stories because this format is simple, and because it makes it explicit why we want each thing.

For the description below it is enough to know know that essentially user stories just describe what functionality you want, who exactly needs it (role), and why you need it.
This helps to avoid requirements that are server little purpose.

The format is usually something similar to this:

> As a <role of a user>
> I want <some functionality>
> so that / in order to <benefit / why you want this>

In our case the role or the type of user is always the same.
Though it feels a bit contrived to describe a specific "role", but sticking to this simple format does make it easier to write the user stories.
We explain the intended users in section

We also leave out other stuff like acceptance criteria because that isn't the point of this section.
Though these extras can be useful, here we are not writing a whole requirements analysis.
We only want to communicate what the goals of this tool are, for example to new developers.

## Intended users for this tool

It comes down to this:

1. We mainly want to use it to create catalogs for openEO, hence the users are openEO users.

2. It should be usable for people who do have technical skills, but you should not have to be a software developer.

For example people who know EO and GIS, and tend to work with fairly complex data would be able to use it.
But we want the keep the amount of configuration you have to do reasonably limited, as well as the amount of custom code you may need to write.

This doesn't mean you will never have to add any new code.
In particulate, some of the file path parsing can be very specific to how your new dataset is organized,
and it is too difficult to build a one-size-fits all solution that has endless flexibility but is also easy to use.

### Summarized: how technical can the tool be?

- It is okay that the use and configuration is a bit technical, but you should not have write a lot of new code for a new dataset.
- It is fine that we need to write a bit of custom code, for example a new class to parse a different GeoTIFF path.
- It does not have to be fool proof, but it should also not be very cumbersome either.

---

## 1. General goal

> - As an openeo user,
> - I want to create STAC collections or catalogs from existing datasets (= collections of EO images), <br/>
> - so that I can use the images in the openeo system, via the load_stac process.

## 2. Find All Input Files via Globbing

### 2.1 Globbing: Including Filee

> - As an openeo user,
> - I want to find all geotiff files a nested directory structure,<br/>
>     with a simple configuration,
> - so that the STAC builder can generate STAC items for all the files.

Note: This can be achieve with a glob pattern to find the files in a directory.

### 2.2 Globbing: Excluding Files

> - As an openeo user,<br/>
> - I want to be able to ignore some files in a nested directory structure,<br/>
> - so that the STAC builder skips files that are not images or that are not part of the bands I want to include..

> **Note**:
> This can also be achieve with a glob pattern to find the files in a directory, just by make the glob specific enough.

So when it comes down to it, this would not be different from finding the files you want to include, described in [2.2 Globbing: Include](#21-globbing-include) .
In principle you only have to make the glob pattern specific enough.
If however, this is turns out to be too cumbersome then we can decide to add some option to make it easier.

## 3 Reusing metadata in source images where possible

> - As an openeo user,
> - I want the STAC builder to use the metadata that is already in the images,
> - so that I don't have to fill in too many fields in the configuration files.

## 4 metadata configuration

### 4.1 metadata configuration: options for missing metadata / metadata that can not be read or derived.

Some metadata can be taken from the source images or derived from known data, but there will always be something we have to tell the tool to fill in.

> - As an openeo user,
> - I want to fill in missing metadata in the STAC collection and STAC items,
> - so that it is easy to have all required metadata for valid STAC catalogs, as well as to use the files in openeo.

### 4.2 metadata configuration: keep configuration relatively easy, low amount of code.

> - As an openeo user,
> - I want a relatively easy to use configuration to specify what to fill in for the missing metadata,
> - so that I don't have to write or modify too much code for each new dataset.

### 4.3 metadata configuration, "directory layout": group STAC items files together

If there are vey many (GeoTIFF / netCDF) source files then there are often in a directory structure
to avoid we have to dig through very long lists of files to find a specific raster file.
We want to do the same for the STAC file, make it possible to structure the directory to keep it manageable for humans to find the STAC file, when they need to.

Ideally we would reproduce the same structure as the source files, but that may be too complicated to implement for what value it adds. It is fine that we have to define the directory structure,
as long as it is easy to do that.

Note: This directory layout is already supported in PySTAC and we will use that.

> - As an openeo user,
> - I want some control over how the STAC catalog (the STAC items) are stored in a directory structure
> - so that they can be grouped in the most useful structure without too much work.

## 5 Group assets that belong together into one STAC Item

We may have several file that belong to one STAC Item, for example a GeoTIFF for each band, and we want to bundle all these assets into one STAC item.


## 6 Support other sources of data and metadata than GeoTIFF

We already have a request to convert data from OpenSearch into STAC where the organization of the products is more suited to our needs. Basically grouping products where each band is now a separate product into one STAC item that has several bands.

## Split up very larger static STAC collections to prevent long loading times.

If you have very many files (assets) in the order of 10k assets and more, you will find that PySTAC takes a long time to process these.
In that case we want to split up those collections into smaller collections, say one for each year, or one for each month of a year. Then we can group those collections into a STAC catalog.
