from __future__ import annotations
import sys
from narwhals.utils import Version
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from narwhals_daft.namespace import DaftNamespace

def __narwhals_namespace__(version: Version) -> DaftNamespace:
    from narwhals_daft.namespace import DaftNamespace
    return DaftNamespace(version=version)

def is_native_object(native_object:  Any) -> bool:
    if (daft := sys.modules.get('daft', None)) is not None:
        return isinstance(native_object, daft.DataFrame)
    return False
