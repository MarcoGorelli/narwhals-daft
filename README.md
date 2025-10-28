# Narwhals-daft

Narwhals extension for [Daft](https://github.com/Eventual-Inc/Daft).

Work-in-progress.

## Installation

Clone the repository, make a virtual environment, install Narwhals, Daft, and `narwhals_daft`:

```console
uv venv
. .venv/bin/activate
uv pip install -U daft -e .
```
If you want to do development work on narwhals-daft, also install the plugin itself:

`uv pip install -e . --group dev`

Git clone Narwhals and install it locally:

```
git clone git@github.com:narwhals-dev/narwhals.git
uv pip install -U -e narwhals --group tests
```

You can verify the install has worked with `python run.py`, you should get the required outputs.

If you want to use the narwhals test suite:

```
pytest narwhals/tests/expr_and_series/abs_test.py -v -p pytest_constructor_override --use-external-constructor
```

The first time you run it, it may complain about `pandas` not being installed. After installation of the package and re-running the above command you currently should get:

```console
collected 2 items

narwhals/tests/expr_and_series/abs_test.py::test_abs[daft] PASSED                                                                   [ 50%]
narwhals/tests/expr_and_series/abs_test.py::test_abs_series[NOTSET] SKIPPED (got empty parameter set for (constructor_eager))       [100%]

====================================================== 1 passed, 1 skipped in 0.52s =======================================================
```

## Usage

Example usage is in `run.py`:

```python
python run.py
```

## Work that needs doing

In no particular order:

- Check whether `Implementation.UNKNOWN` is alright or if it could/should be improved.
- Implement `group_by`, `selectors`, and other missing parts. INPROG
- See if there's an easy way to re-use the Narwhals test suite. INPROG
