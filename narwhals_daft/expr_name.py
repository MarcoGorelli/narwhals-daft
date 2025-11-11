from __future__ import annotations

from typing import TYPE_CHECKING

from narwhals._compliant.expr import CompliantExprNameNamespace

if TYPE_CHECKING:
    from narwhals._compliant.typing import AliasName

    from narwhals_daft.expr import DaftExpr


class ExprNameNamespace(CompliantExprNameNamespace["DaftExpr"]):
    def __init__(self, expr: DaftExpr, /) -> None:
        self._compliant_expr = expr

    def _from_callable(self, func: AliasName | None) -> DaftExpr:
        expr = self.compliant
        output_names = self._alias_output_names(func) if func else None
        return expr._with_alias_output_names(output_names)
