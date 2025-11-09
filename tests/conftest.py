from __future__ import annotations

from typing import Any

import daft
import pytest


@pytest.fixture
def constructor(data: dict[str, Any]) -> daft.DataFrame:
    return daft.from_pydict(data)
