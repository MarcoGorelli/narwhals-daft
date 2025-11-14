from __future__ import annotations

from typing import TYPE_CHECKING

import daft.functions as F
from narwhals._compliant.any_namespace import StringNamespace
from narwhals._utils import not_implemented

if TYPE_CHECKING:
    from narwhals_daft.expr import DaftExpr


class ExprStringNamespace(StringNamespace["DaftExpr"]):
    def __init__(self, expr: DaftExpr, /) -> None:
        self._compliant = expr

    @property
    def compliant(self) -> DaftExpr:
        return self._compliant

    def len_chars(self) -> DaftExpr:
        return self.compliant._with_elementwise(lambda expr: F.length(expr))

    def to_lowercase(self) -> DaftExpr:
        return self.compliant._with_elementwise(lambda expr: F.lower(expr))

    def to_uppercase(self) -> DaftExpr:
        return self.compliant._with_elementwise(lambda expr: F.upper(expr))

    def to_date(self, format: str | None = None) -> DaftExpr:
        if format is None:
            format = "%Y-%m-%d"
        return self.compliant._with_elementwise(lambda expr: F.to_date(expr, format))

    def split(self, by: str) -> DaftExpr:
        return self.compliant._with_elementwise(lambda expr: F.split(expr, by))

    replace = not_implemented()
    replace_all = not_implemented()
    strip_chars = not_implemented()
    starts_with = not_implemented()
    ends_with = not_implemented()
    contains = not_implemented()
    slice = not_implemented()
    to_datetime = not_implemented()
    to_titlecase = not_implemented()
    zfill = not_implemented()
