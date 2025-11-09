from __future__ import annotations

from typing import TYPE_CHECKING

from narwhals._compliant import CompliantSelector, LazySelectorNamespace

from narwhals_daft.expr import DaftExpr

if TYPE_CHECKING:
    from daft import Expression  # noqa: F401

    from narwhals_daft.dataframe import DaftLazyFrame  # noqa: F401
    from narwhals_daft.expr import DaftWindowFunction


class DaftSelectorNamespace(LazySelectorNamespace["DaftLazyFrame", "Expression"]):
    @property
    def _selector(self) -> type[DaftSelector]:
        return DaftSelector


class DaftSelector(  # type: ignore[misc]
    CompliantSelector["DaftLazyFrame", "Expression"], DaftExpr
):
    _window_function: DaftWindowFunction | None = None

    def _to_expr(self) -> DaftExpr:
        return DaftExpr(
            self._call,
            self._window_function,
            evaluate_output_names=self._evaluate_output_names,
            alias_output_names=self._alias_output_names,
            version=self._version,
        )
