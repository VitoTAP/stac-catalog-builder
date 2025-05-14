
# Setup - Installation
There are two ways to install the STAC catalog builder tool:
- Use [uv](https://docs.astral.sh/uv/) to install the tool in a virtualenv.
- Create your own virtualenv and install the package manually.

## Using `uv` to install the tool in a virtualenv

UV is a tool to create and manage virtual environments. It is a bit like conda, but it is much smaller and faster. It is also more flexible and has a lot of features that are not in conda.

This project comes with a uv.lock file that contains the dependencies for the project and their exact versions and sources.

Start by installing uv using their getting started instructions: https://docs.astral.sh/uv/getting-started/installation/

After that you can install the tool using the following command:

```bash
uv sync
```

This will create a new virtual environment in the `.venv` folder and install all the dependencies in it.

## Installing the tool manually

This project requires Python 3.11 or higher. It is recommended to use a virtual environment to avoid conflicts with other packages.

In your vitual environment, you can install the package using pip:

```bash
pip install -e .
```

## For developers: installing the dependencies

To install the development dependencies, you can use the following command:

```bash
uv sync --dev
```
or

```bash
pip install -e .[dev]
```

More information on contributing to the project can be found in the [Developer Guidelines](docs/developer-guidelines.md).
