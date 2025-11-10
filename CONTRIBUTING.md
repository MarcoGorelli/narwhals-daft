# Contributing

Clone this repository with the `--recursive` flag.

```console
git clone git@github.com:narwhals-dev/narwhals-daft.git narwhals-daft-dev --recursive 
cd narwhals-daft-dev
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

## Updating the Narwhals submodule

Run

```console
. utils/submodule_update.sh
```

Check that tests still run, update `run_tests.sh` if necessary, and open a PR.
