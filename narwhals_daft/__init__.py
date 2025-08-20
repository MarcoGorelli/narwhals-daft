from narwhals_daft import dataframe
from narwhals_daft import expr as expr
import daft
from narwhals.utils import Version
from typing import Any
from typing_extensions import TypeIs

def from_native(native_object: daft.DataFrame, eager_only: bool, series_only: bool) -> dataframe.DaftLazyFrame:
    if eager_only or series_only:
        raise ValueError("eager_only and series_only options are not supported as daft is lazy-only.")
    return dataframe.DaftLazyFrame(native_object, version=Version.MAIN)

def is_native_object(obj: Any) -> TypeIs[daft.DataFrame]:
    return isinstance(obj, daft.DataFrame)
