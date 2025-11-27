from __future__ import annotations

from typing import TYPE_CHECKING

import daft.functions as F
from narwhals._compliant.any_namespace import DateTimeNamespace
from narwhals._utils import not_implemented

if TYPE_CHECKING:
    from narwhals_daft.expr import DaftExpr


class ExprDateTimeNamesSpace(DateTimeNamespace["DaftExpr"]):
    def __init__(self, expr: DaftExpr, /) -> None:
        self._compliant = expr

    @property
    def compliant(self) -> DaftExpr:
        return self._compliant

    def date(self) -> DaftExpr:
        return self.compliant._with_elementwise(lambda expr: F.date(expr))

    to_string = not_implemented()
    replace_time_zone = not_implemented()
    convert_time_zone = not_implemented()
    timestamp = not_implemented()
    year = not_implemented()
    month = not_implemented()
    day = not_implemented()
    hour = not_implemented()
    minute = not_implemented()
    second = not_implemented()
    millisecond = not_implemented()
    microsecond = not_implemented()
    nanosecond = not_implemented()
    ordinal_day = not_implemented()
    weekday = not_implemented()
    total_minutes = not_implemented()
    total_seconds = not_implemented()
    total_milliseconds = not_implemented()
    total_microseconds = not_implemented()
    total_nanoseconds = not_implemented()
    truncate = not_implemented()
    offset_by = not_implemented()
