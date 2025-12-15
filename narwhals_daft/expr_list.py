from __future__ import annotations

from typing import TYPE_CHECKING

import daft.functions as F
from narwhals._compliant.any_namespace import ListNamespace
from narwhals._utils import not_implemented

if TYPE_CHECKING:
    from narwhals_daft.expr import DaftExpr


class ExprListNamespace(ListNamespace["DaftExpr"]):
    def __init__(self, expr: DaftExpr, /) -> None:
        self._compliant = expr

    @property
    def compliant(self) -> DaftExpr:
        return self._compliant

    def len(self) -> DaftExpr:
        return self.compliant._with_elementwise(lambda expr: F.list_count(expr, "all"))

    def min(self) -> DaftExpr:
        return self.compliant._with_elementwise(lambda expr: F.list_min(expr))

    def max(self) -> DaftExpr:
        return self.compliant._with_elementwise(lambda expr: F.list_max(expr))

    def mean(self) -> DaftExpr:
        return self.compliant._with_elementwise(lambda expr: F.list_mean(expr))

    unique = not_implemented()
    contains = not_implemented()
    get = not_implemented()
    median = not_implemented()
    sum = not_implemented()
