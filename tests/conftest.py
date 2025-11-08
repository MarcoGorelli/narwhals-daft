import pytest
import daft


@pytest.fixture
def constructor(data):
    return daft.from_pydict(data)
