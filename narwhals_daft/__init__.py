from __future__ import annotations
import sys
from narwhals_daft import dataframe
from narwhals_daft import expr as expr
from narwhals_daft.namespace import DaftNamespace
from narwhals.utils import Version
from typing import TYPE_CHECKING, Any

# if TYPE_CHECKING:
#     import daft

def __narwhals_namespace__(unclear:Any) -> DaftNamespace:
    return DaftNamespace.from_native(native_object)

def is_native_object(native_object:  daft.DataFrame) -> bool:
    if (daft := sys.modules.get('daft', None)) is not None:
        return isinstance(native_object, daft.DataFrame)
    return False
