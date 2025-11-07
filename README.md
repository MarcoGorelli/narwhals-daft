# Narwhals-daft

Narwhals extension for [Daft](https://github.com/Eventual-Inc/Daft).

Work-in-progress.

## Installation

Clone this repository with the `--recursive` flag. This installs Narwhals as a git
submodule.

```console
git clone --recursive <project url>
```

Make a virtual environment, install Daft,
Narwhals, and `narwhals_daft`:

```console
uv venv
. .venv/bin/activate
uv pip install -U daft -e .
```

If you want to do development work on narwhals-daft, install the plugin itself
with relevant flags:

```console
uv pip install -e . --group tests
```

and also fetch the submodule:

```console
git submodule update --init --recursive
```

To run the tests:

```console
. run_tests.sh
```

Any additional arguments you pass will be passed down to pytest, e.g.

```console
. run_tests.sh -x
```
