from __future__ import annotations

from typing import TYPE_CHECKING

import daft.functions as F
from narwhals._compliant import LazyExprNamespace
from narwhals._compliant.any_namespace import ListNamespace
from narwhals._utils import not_implemented

if TYPE_CHECKING:
    from narwhals_daft.expr import DaftExpr

class ExprListNamespace(LazyExprNamespace["DaftExpr"], ListNamespace["DaftExpr"]):

    def __init__(self, expr: DaftExpr, /) -> None:
        self._compliant = expr

    @property
    def compliant(self) -> DaftExpr:
        return self._compliant

    def max(self) -> DaftExpr:
        return self.compliant._with_elementwise(lambda expr: F.list_max(expr))

    len = not_implemented()
    unique = not_implemented()
    contains = not_implemented()
    get = not_implemented()
    min = not_implemented()
    # max = not_implemented()
    mean = not_implemented()
    median = not_implemented()
    sum = not_implemented()
