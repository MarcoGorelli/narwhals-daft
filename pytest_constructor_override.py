import pytest
import daft


@pytest.hookimpl(tryfirst=True)
def pytest_generate_tests(metafunc):
    if "constructor" in metafunc.fixturenames:
        metafunc.parametrize("constructor", [daft_constructor], ids=["daft"])
    if "constructor_eager" in metafunc.fixturenames:
        metafunc.parametrize("constructor_eager", [], ids=[])


def daft_constructor(data):
    return daft.from_pydict(data)
