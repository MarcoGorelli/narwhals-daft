from __future__ import annotations

from typing import TYPE_CHECKING

from narwhals.compliant import CompliantSelector, CompliantSelectorNamespace

from narwhals_daft.expr import DaftExpr

if TYPE_CHECKING:
    from collections.abc import Iterator

    from daft import Expression
    from narwhals.dtypes import DType

    from narwhals_daft.dataframe import DaftLazyFrame
    from narwhals_daft.expr import WindowFunction


class DaftSelectorNamespace(CompliantSelectorNamespace["DaftLazyFrame", "Expression"]):
    @property
    def _selector(self) -> type[DaftSelector]:
        return DaftSelector

    def _iter_schema(self, df: DaftLazyFrame) -> Iterator[tuple[str, DType]]:
        yield from df.schema.items()

    def _iter_columns(self, df: DaftLazyFrame) -> Iterator[Expression]:
        yield from df._iter_columns()

    def _iter_columns_dtypes(
        self, df: DaftLazyFrame, /
    ) -> Iterator[tuple[Expression, DType]]:
        yield from zip(self._iter_columns(df), df.schema.values(), strict=True)


class DaftSelector(CompliantSelector["DaftLazyFrame", "Expression"], DaftExpr):
    _window_function: WindowFunction | None = None

    def _to_expr(self) -> DaftExpr:
        return DaftExpr(
            self._call,
            self._window_function,
            evaluate_output_names=self._evaluate_output_names,
            alias_output_names=self._alias_output_names,
            version=self._version,
        )
