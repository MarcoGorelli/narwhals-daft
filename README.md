# Narwhals-daft

Narwhals extension for [Daft](https://github.com/Eventual-Inc/Daft).

Work-in-progress.

## Installation from source

Clone this repository with the `--recursive` flag. This installs Narwhals as a git
submodule.

```console
git clone --recursive <project url>
```

```console
uv pip install -e . --group tests
```

and also fetch and install Narwhals as a git submodule:

```console
git submodule update --init --recursive
uv pip install -e narwhals
```

To run the tests:

```console
. run_tests.sh
```

Any additional arguments you pass will be passed down to pytest, e.g.

```console
. run_tests.sh -x
```

To run type-checking:

```console
pyright narwhals_daft
```
