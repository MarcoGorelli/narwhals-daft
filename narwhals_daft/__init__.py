from __future__ import annotations
import sys
from narwhals_daft import dataframe
from narwhals_daft import expr as expr
from narwhals_daft.namespace import DaftNamespace
from narwhals.utils import Version
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import daft

def __getattr__(name: str) -> Any:
    if name == "from_native":
        from narwhals_daft.namespace import DaftNamespace

        from narwhals._utils import Version

        return DaftNamespace(version=Version.MAIN).from_native
    msg = f"module {__name__!r} has no attribute {name!r}"
    raise AttributeError(msg)

# but here we still need daft?!?
def is_native_object(native_object:  daft.DataFrame) -> bool:
    if (daft := sys.modules.get('daft', None)) is not None:
        return isinstance(native_object, daft.DataFrame)
    return False
