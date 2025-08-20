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

Then, install Narwhals from this branch: https://github.com/ym-pett/narwhals/tree/create_fromnative_daft, which we plan to merge into Narwhals shortly.

## Usage

Example usage is in `run.py`:

```python
python run.py
```

## Work that needs doing

In no particular order:

- Check whether `Implementation.UNKNOWN` is alright or if it could/should be improved.
- Implement `group_by`, `selectors`, and other missing parts. INPROG
- See if there's an easy way to re-use the Narwhals test suite.

