from __future__ import annotations

from typing import TYPE_CHECKING

import daft.functions as F
from narwhals._compliant.any_namespace import DateTimeNamespace
from narwhals._compliant.column import CompliantColumn
from narwhals._utils import not_implemented

if TYPE_CHECKING:
    from narwhals_daft.expr import DaftExpr


class DaftExprDateTimeNamesSpace(CompliantColumn, DateTimeNamespace[DaftExpr]):
    
    def date(self) -> DaftExpr:
        return self.compliant._with_elementwise(
            lambda expr: F.date(expr)
        )
    
    year = not_implemented()
    month = not_implemented()
    day = not_implemented()
    hour = not_implemented()
    minute = not_implemented()
    second = not_implemented()
    ordinal_day = not_implemented()
