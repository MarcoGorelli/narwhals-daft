from __future__ import annotations

from typing import TYPE_CHECKING

from narwhals._compliant import CompliantSelector, LazySelectorNamespace
from narwhals_daft.expr import DaftExpr

if TYPE_CHECKING:
    from dataframe.base import Column  

    from narwhals_daft.dataframe import DaftLazyFrame  
    from narwhals_daft.expr import 