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

    replace = not_implemented()
    replace_all = not_implemented()
    strip_chars = not_implemented()
    starts_with = not_implemented()
    ends_with = not_implemented()
    contains = not_implemented()
    slice = not_implemented()
    split = not_implemented()
    to_datetime = not_implemented()
    to_date = not_implemented()
    to_lowercase = not_implemented()
    to_titlecase = not_implemented()
    to_uppercase = not_implemented()
    zfill = not_implemented()
