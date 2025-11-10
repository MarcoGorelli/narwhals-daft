from __future__ import annotations

from typing import TYPE_CHECKING

from narwhals._compliant.group_by import CompliantGroupBy, ParseKeysGroupBy

if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator, Sequence

    from daft import Expression

    from narwhals_daft.dataframe import DaftLazyFrame
    from narwhals_daft.expr import DaftExpr


class DaftLazyGroupBy(
    ParseKeysGroupBy["DaftLazyFrame", "DaftExpr"],
    CompliantGroupBy["DaftLazyFrame", "DaftExpr"],
):
    def __init__(
        self,
        df: DaftLazyFrame,
        keys: Sequence[DaftExpr] | Sequence[str],
        /,
        *,
        drop_null_keys: bool,
    ) -> None:
        frame, self._keys, self._output_key_names = self._parse_keys(df, keys=keys)
        self._compliant_frame = (
            frame.drop_nulls(self._keys) if drop_null_keys else frame
        )

    def _evaluate_expr(self, expr: DaftExpr, /) -> Iterator[Expression]:
        output_names = expr._evaluate_output_names(self.compliant)
        aliases = (
            expr._alias_output_names(output_names)
            if expr._alias_output_names
            else output_names
        )
        native_exprs = expr(self.compliant)
        if expr._is_multi_output_unnamed():
            exclude = {*self._keys, *self._output_key_names}
            for native_expr, name, alias in zip(
                native_exprs, output_names, aliases, strict=True
            ):
                if name not in exclude:
                    yield expr._alias_native(native_expr, alias)
        else:
            for native_expr, alias in zip(native_exprs, aliases, strict=True):
                yield expr._alias_native(native_expr, alias)

    def _evaluate_exprs(self, exprs: Iterable[DaftExpr], /) -> Iterator[Expression]:
        for expr in exprs:
            yield from self._evaluate_expr(expr)

    def agg(self, *exprs: DaftExpr) -> DaftLazyFrame:
        result = (
            self.compliant.native.groupby(*self._keys).agg(*agg_columns)
            if (agg_columns := tuple(self._evaluate_exprs(exprs)))
            else self.compliant.native.select(*self._keys).drop_duplicates()
        )

        return self.compliant._with_native(result).rename(
            dict(zip(self._keys, self._output_key_names, strict=True))
        )
