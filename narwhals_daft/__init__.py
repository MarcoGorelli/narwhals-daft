import sys
from narwhals_daft import dataframe
from narwhals_daft import expr as expr
from narwhals.utils import Version
from typing import Any, TYPE_CHECKING
from typing_extensions import TypeIs

if TYPE_CHECKING:
    import daft

def from_native(native_object: "daft.DataFrame", eager_only: bool, series_only: bool) -> dataframe.DaftLazyFrame:
    if eager_only or series_only:
        raise ValueError("eager_only and series_only options are not supported as daft is lazy-only.")
    return dataframe.DaftLazyFrame(native_object, version=Version.MAIN)

def is_native_object(native_object:  "daft.DataFrame") -> bool:
    if (daft := sys.modules.get('daft', None)) is not None:
        return isinstance(native_object, daft.DataFrame)
    return False

# # longer form for personal understanding:
# def is_native_object(native_object:  "daft.DataFrame") -> bool:
#     daft = sys.modules.get("daft")
#     if daft is not None:
#         return isinstance(native_object, daft.DataFrame)
#     return False

