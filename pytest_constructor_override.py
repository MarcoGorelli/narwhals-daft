from __future__ import annotations

from typing import Any

import daft
import pytest


@pytest.hookimpl(tryfirst=True)
def pytest_generate_tests(metafunc: pytest.Metafunc) -> None:
    if "constructor" in metafunc.fixturenames:
        metafunc.parametrize("constructor", [daft_constructor], ids=["daft"])
    if "constructor_eager" in metafunc.fixturenames:
        metafunc.parametrize("constructor_eager", [], ids=[])


def daft_constructor(data: dict[str, Any]) -> daft.DataFrame:
    return daft.from_pydict(data)
