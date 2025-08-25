from __future__ import annotations
import sys
from narwhals_daft import dataframe
from narwhals_daft import expr as expr
from narwhals.utils import Version
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import daft

def from_native(native_object: daft.DataFrame, version: Version) -> dataframe.DaftLazyFrame:
    return dataframe.DaftLazyFrame(native_object, version=version)

def is_native_object(native_object:  daft.DataFrame) -> bool:
    if (daft := sys.modules.get('daft', None)) is not None:
        return isinstance(native_object, daft.DataFrame)
    return False
