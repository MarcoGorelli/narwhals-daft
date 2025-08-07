# Narwhals-daft

Narwhals extension for [Daft](https://github.com/Eventual-Inc/Daft).

Work-in-progress.

## Installation

Clone the repository, make a virtual environment, install Narwhals, Daft, and `narwhals_daft`:

```console
uv venv
. .venv/bin/activate
uv pip install -U daft narwhals -e .
```

## Usage

Example usage is in `run.py`: 

```python
python run.py
```

## Work that needs doing

In no particular order:

- If you install `pyright` (`uv pip install pyright`) and then run `pyright narwhals_daft`, you'll
  get some errors that methods like `__or__` and others are not implemented. So, first, implement
  those, see `__mul__` for an example.
- Usability: in `run.py`, we shoud aim to let users pass a `daft.DataFrame` directly to `nw.from_native`.
  This will require some changes in Narwhals.
- Check that `nw.from_native` returns the correct type (`LazyFrame` or `DataFrame`).
- Check whether `Implementation.UNKNOWN` is alright or if it could/should be improved.
- Is `_evaluate_expr` needed? Can it be fixed in Narwhals first?
- Implement `group_by`, `selectors`, and other missing parts.
- See if there's an easy way to re-use the Narwhals test suite.
