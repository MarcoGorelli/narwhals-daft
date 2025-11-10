from __future__ import annotations

from typing import Any

import daft
import pytest

CONSTRUCTORS_TO_SKIP = ("constructor_eager", "constructor_pandas_like")


@pytest.hookimpl(tryfirst=True)
def pytest_generate_tests(metafunc: pytest.Metafunc) -> None:
    if "constructor" in metafunc.fixturenames:
        metafunc.parametrize("constructor", [daft_constructor], ids=["daft"])
    for constructor in metafunc.fixturenames:
        if constructor in CONSTRUCTORS_TO_SKIP:
            metafunc.parametrize(constructor, [], ids=[])


def daft_constructor(data: dict[str, Any]) -> daft.DataFrame:
    return daft.from_pydict(data)
