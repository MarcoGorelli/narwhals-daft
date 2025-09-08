from __future__ import annotations

import operator
from functools import reduce
from typing import TYPE_CHECKING, Any

import daft
import daft.functions
from daft import Expression

from narwhals._compliant.namespace import LazyNamespace
#from narwhals_daft import dataframe
from narwhals_daft.dataframe import DaftLazyFrame
from narwhals_daft.expr import DaftExpr
from narwhals_daft.utils import lit, narwhals_to_native_dtype
from narwhals._utils import Implementation, not_implemented

if TYPE_CHECKING:
    from collections.abc import Iterable
    from narwhals._utils import Version
    from narwhals.dtypes import DType
    from narwhals.typing import ConcatMethod


class DaftNamespace(LazyNamespace[DaftLazyFrame, DaftExpr, daft.DataFrame]):
    _implementation: Implementation = Implementation.UNKNOWN

    def from_native(self, data: daft.DataFrame | Any, **kwargs: Any) -> DaftLazyFrame:
        if kwargs:
            msg = "eager_only and series_only options are not supported as daft is lazy-only."
            raise ValueError(msg)
        return super().from_native(data)

    def __init__(self, *, version: Version) -> None:
        self._version = version

    @property
    def _expr(self) -> type[DaftExpr]:
        return DaftExpr

    @property
    def _lazyframe(self) -> type[DaftLazyFrame]:
        return DaftLazyFrame

    def lit(self, value: Any, dtype: DType | type[DType] | None) -> DaftExpr:
        def func(_df: DaftLazyFrame) -> list[Expression]:
            if dtype is not None:
                return [lit(value).cast(narwhals_to_native_dtype(dtype, self._version))]
            return [lit(value)]

        return DaftExpr(
            func,
            evaluate_output_names=lambda _df: ["literal"],
            alias_output_names=None,
            version=self._version,
        )

    def concat(
        self, items: Iterable[DaftLazyFrame], *, how: ConcatMethod
    ) -> DaftLazyFrame:
        list_items = list(items)
        native_items = (item._native_frame for item in items)
        if how == "diagonal":
            return DaftLazyFrame(
                reduce(lambda x, y: x.union_all_by_name(y), native_items),
                version=self._version,
            )
        first = list_items[0]
        schema = first.schema
        if how == "vertical" and not all(x.schema == schema for x in list_items[1:]):
            msg = "inputs should all have the same schema"
            raise TypeError(msg)
        res = reduce(lambda x, y: x.union(y), native_items)
        return first._with_native(res)

    concat_str = not_implemented()

    def all_horizontal(self, *exprs: DaftExpr, ignore_nulls: bool) -> DaftExpr:
        def func(cols: Iterable[Expression]) -> Expression:
            it = (
                (daft.coalesce(col, lit(True)) for col in cols)  # noqa: FBT003
                if ignore_nulls
                else cols
            )
            return reduce(operator.and_, it)

        return self._expr._from_elementwise_horizontal_op(func, *exprs)

    def any_horizontal(self, *exprs: DaftExpr, ignore_nulls: bool) -> DaftExpr:
        def func(cols: Iterable[Expression]) -> Expression:
            it = (
                (daft.coalesce(col, lit(False)) for col in cols)  # noqa: FBT003
                if ignore_nulls
                else cols
            )
            return reduce(operator.or_, it)

        return self._expr._from_elementwise_horizontal_op(func, *exprs)

    def sum_horizontal(self, *exprs: DaftExpr) -> DaftExpr:
        def func(cols: Iterable[Expression]) -> Expression:
            return daft.functions.columns_sum(*cols)

        return self._expr._from_elementwise_horizontal_op(func, *exprs)

    def max_horizontal(self, *exprs: DaftExpr) -> DaftExpr:
        def func(cols: Iterable[Expression]) -> Expression:
            return daft.functions.columns_max(*cols)

        return self._expr._from_elementwise_horizontal_op(func, *exprs)

    def min_horizontal(self, *exprs: DaftExpr) -> DaftExpr:
        def func(cols: Iterable[Expression]) -> Expression:
            return daft.functions.columns_min(*cols)

        return self._expr._from_elementwise_horizontal_op(func, *exprs)

    def mean_horizontal(self, *exprs: DaftExpr) -> DaftExpr:
        def func(cols: Iterable[Expression]) -> Expression:
            return daft.functions.columns_mean(*cols)

        return self._expr._from_elementwise_horizontal_op(func, *exprs)

    def len(self) -> DaftExpr:
        def func(_df: DaftLazyFrame) -> list[Expression]:
            if not _df.columns:  # pragma: no cover
                msg = "Cannot use `nw.len()` on Daft DataFrame with zero columns"
                raise ValueError(msg)
            return [daft.col(_df.columns[0]).count(mode="all")]

        return DaftExpr(
            call=func,
            evaluate_output_names=lambda _df: ["len"],
            alias_output_names=None,
            version=self._version,
        )

    when: not_implemented = not_implemented()
    coalesce: not_implemented = not_implemented()
    selectors: not_implemented = not_implemented()
