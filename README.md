# Narwhals-daft

Narwhals extension for [Daft](https://github.com/Eventual-Inc/Daft).

Work-in-progress.

## Installation

Clone this repository with the `--recursive` flag. This installs Narwhals as a git
submodule.

`git clone --recursive <project url>`

Make a virtual environment, install Daft,
Narwhals, and `narwhals_daft`:

```console
uv venv
. .venv/bin/activate
uv pip install -U daft -e .
```
If you want to do development work on narwhals-daft, also install the plugin itself:

`uv pip install -e . --group tests`

To install `narwhals`:

```console
uv pip install -U -e narwhals --group dev
```

You can verify the install has worked with `python run.py`, you should get Narwhals
LazyFrames as outputs.

If you want to use the narwhals test suite:

```
pytest narwhals/tests/expr_and_series/abs_test.py -v -p pytest_constructor_override --use-external-constructor
```

You currently should get:

```console
collected 2 items

narwhals/tests/expr_and_series/abs_test.py::test_abs[daft] PASSED                                                                   [ 50%]
narwhals/tests/expr_and_series/abs_test.py::test_abs_series[NOTSET] SKIPPED (got empty parameter set for (constructor_eager))       [100%]

====================================================== 1 passed, 1 skipped in 0.52s =======================================================
```

## Usage

Example usage is in `run.py`:

```console
python run.py
```

## Work that needs doing

In no particular order:

- Check whether `Implementation.UNKNOWN` is alright or if it could/should be improved.
- Implement `group_by`, `selectors`, and other missing parts. INPROG
- See if there's an easy way to re-use the Narwhals test suite. INPROG
