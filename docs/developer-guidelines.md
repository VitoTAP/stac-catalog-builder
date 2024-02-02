# Software Development Guidelines

The packaging is based on Hatch and Hatchling.

[Hatch docs: https://hatch.pypa.io/](https://hatch.pypa.io/)

## pre-commit

This project uses [pre-commit](https://pre-commit.com/) and [ruff](https://docs.astral.sh/ruff/) to keep the coding style consistent and catch some common mistakes early.

Pre-commit automates the linting and formatting tools and makes sure they are run before the code is committed into git. It is named after the pre-commit hook of git.

### Install pre-commit

```shell
pre-commit install
```

## Build the Python wheel

This commands builds the wheel, and also the source distribution.
You can use the wheel to install the tool if you are not doing any development on it or if you want to containerize it.

```shell
hatch build
```

## Versioning

The version number is stored in stacbuilder._version.py

Update the version:
You can either manually update the version in the file or use Hatch;

You can update the version as described in the [Hatch docs about this topic](https://hatch.pypa.io/latest/version/#updating)

Some examples:

```shell
hatch version "0.1.0"
# Or
hatch version minor
# Or
hatch version major,rc
```
