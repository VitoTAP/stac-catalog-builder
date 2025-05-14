# Software Development Guidelines

We follow the same guidelines as openeo.org. But below we mention a few specific points to make them easier to find.

[Openeo: Software Developer Guidelines: https://openeo.org/documentation/software-guidelines.html](https://openeo.org/documentation/software-guidelines.html)

Instruction on how to set up the development environment can be found in the [installation guide](installation.md).

## pre-commit

This project uses [pre-commit](https://pre-commit.com/) and [ruff](https://docs.astral.sh/ruff/) to keep the coding style consistent and catch some common mistakes early.

Pre-commit automates the linting and formatting tools and makes sure they are run before the code is committed into git. It is named after the pre-commit hook of git.

### Install pre-commit
```shell
uv run pre-commit install
```
or if you have installed the package in your own virtual environnement:

```shell
pre-commit install
```

### Run pre-commit manually
Pre-commit is run automatically when you commit code on your staged files. But you can also run it manually to check all files.

```shell
uv run pre-commit run --all-files
```
or if you have installed the package in your own virtual environnement:

```shell
pre-commit run --all-files
```

## Running tests
This packages uses [pytest](https://docs.pytest.org/en/latest/) for testing.
You can run the tests with:

```shell
uv run pytest
```
or if you have installed the package in your own virtual environnement:

```shell
pytest
```


## Packaging and building the Python Wheel

The packaging is based on Hatch and Hatchling.

[Hatch docs: https://hatch.pypa.io/](https://hatch.pypa.io/)

This commands builds the wheel, and also the source distribution.
You can use the wheel to install the tool if you are not doing any development on it or if you want to containerize it.

```shell
hatch build
```

Help about hatch

```shell
hatch --help
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
