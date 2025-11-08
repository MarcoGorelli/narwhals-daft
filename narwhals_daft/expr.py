from __future__ import annotations

import operator
from collections.abc import Iterable
from typing import TYPE_CHECKING, Any, Callable, cast

from daft import coalesce, col, lit, Window

from narwhals._compliant import LazyExpr
from narwhals._compliant.window import WindowInputs  # todo: make public?
from narwhals_daft.utils import narwhals_to_native_dtype
from narwhals._expression_parsing import (
    combine_alias_output_names,
    combine_evaluate_output_names,
)
from narwhals._utils import Implementation, not_implemented

if TYPE_CHECKING:
    from collections.abc import Iterable, Sequence

    from daft import Expression
    from typing_extensions import Self, TypeIs

    from narwhals._compliant.typing import (
        AliasNames,
        EvalNames,
        EvalSeries,
        WindowFunction,
    )
    from narwhals_daft.dataframe import DaftLazyFrame
    from narwhals_daft.namespace import DaftNamespace
    from narwhals._utils import Version, _LimitedContext
    from narwhals.dtypes import DType

    DaftWindowFunction = WindowFunction[DaftLazyFrame, Expression]


class DaftExpr(LazyExpr["DaftLazyFrame", "Expression"]):
    _implementation = Implementation.UNKNOWN

    def __init__(
        self,
        call: Callable[[DaftLazyFrame], Sequence[Expression]],
        window_function: DaftWindowFunction | None = None,
        *,
        evaluate_output_names: EvalNames[DaftLazyFrame],
        alias_output_names: AliasNames | None,
        version: Version,
    ) -> None:
        self._call = call
        self._evaluate_output_names = evaluate_output_names
        self._alias_output_names = alias_output_names
        self._version = version
        self._window_function: DaftWindowFunction | None = window_function

    def _partition_by(self, *cols: Expression | str) -> Window:
        """Wraps `Window().partitionBy`, with default and `WindowInputs` handling."""
        return Window().partition_by(*cols)

    def _window_expression(
        self,
        expr: Expression,
        partition_by: Sequence[str | Expression] = (),
        order_by: Sequence[str | Expression] = (),
        rows_start: int | None = None,
        rows_end: int | None = None,
        *,
        descending: list[bool] | None = None,
        nulls_first: list[bool] | None = None,
    ) -> Expression:
        window = self._partition_by(*partition_by)
        if order_by:
            window = window.order_by(
                *order_by,
                desc=descending or [False] * len(order_by),
                nulls_first=nulls_first or [True] * len(order_by),
            )
        if rows_start is not None and rows_end is not None:
            window = window.rows_between(rows_start, rows_end)
        elif rows_end is not None:
            window = window.rows_between(Window.unbounded_preceding, rows_end)
        elif rows_start is not None:  # pragma: no cover
            window = window.rows_between(rows_start, Window.unbounded_following)
        return expr.over(window)

    @property
    def window_function(self) -> WindowFunction[DaftLazyFrame, Expression]:
        def default_window_func(
            df: DaftLazyFrame, inputs: WindowInputs[Expression]
        ) -> Sequence[Expression]:
            assert not inputs.order_by  # noqa: S101
            return [
                self._window_expression(expr, inputs.partition_by) for expr in self(df)
            ]

        return self._window_function or default_window_func

    def broadcast(self) -> Self:
        return self.over([lit(1)], [])

    def __call__(self, df: DaftLazyFrame) -> Sequence[Expression]:
        return self._call(df)

    def __narwhals_expr__(self) -> None: ...

    def __narwhals_namespace__(self) -> DaftNamespace:  # pragma: no cover
        # Unused, just for compatibility with PandasLikeExpr
        from narwhals_daft.namespace import DaftNamespace

        return DaftNamespace(version=self._version)

    @classmethod
    def _alias_native(cls, expr: Expression, name: str) -> Expression:
        return expr.alias(name)

    @classmethod
    def from_column_names(
        cls: type[Self],
        evaluate_column_names: EvalNames[DaftLazyFrame],
        /,
        *,
        context: _LimitedContext,
    ) -> Self:
        def func(df: DaftLazyFrame) -> list[Expression]:
            return [col(col_name) for col_name in evaluate_column_names(df)]

        return cls(
            func,
            evaluate_output_names=evaluate_column_names,
            alias_output_names=None,
            version=context._version,
        )

    @classmethod
    def from_column_indices(
        cls: type[Self], *column_indices: int, context: _LimitedContext
    ) -> Self:
        def func(df: DaftLazyFrame) -> list[Expression]:
            columns = df.columns
            return [col(columns[i]) for i in column_indices]

        return cls(
            func,
            evaluate_output_names=lambda df: [df.columns[i] for i in column_indices],
            alias_output_names=None,
            version=context._version,
        )

    @classmethod
    def _from_elementwise_horizontal_op(
        cls, func: Callable[[Iterable[Expression]], Expression], *exprs: Self
    ) -> Self:
        def call(df: DaftLazyFrame) -> list[Expression]:
            cols = (col for _expr in exprs for col in _expr(df))
            return [func(cols)]

        context = exprs[0]
        return cls(
            call=call,
            evaluate_output_names=combine_evaluate_output_names(*exprs),
            alias_output_names=combine_alias_output_names(*exprs),
            version=context._version,
        )

    def _callable_to_eval_series(
        self, call: Callable[..., Expression], /, **expressifiable_args: Self | Any
    ) -> EvalSeries[DaftLazyFrame, Expression]:
        def func(df: DaftLazyFrame) -> list[Expression]:
            native_series_list = self(df)
            other_native_series = {
                key: df._evaluate_expr(value) if self._is_expr(value) else lit(value)
                for key, value in expressifiable_args.items()
            }
            return [
                call(native_series, **other_native_series)
                for native_series in native_series_list
            ]

        return func

    def _with_callable(
        self, call: Callable[..., Expression], /, **expressifiable_args: Self | Any
    ) -> Self:
        return self.__class__(
            self._callable_to_eval_series(call, **expressifiable_args),
            evaluate_output_names=self._evaluate_output_names,
            alias_output_names=self._alias_output_names,
            version=self._version,
        )

    def _with_elementwise(
        self, call: Callable[..., Expression], /, **expressifiable_args: Self | Any
    ) -> Self:
        return self.__class__(
            self._callable_to_eval_series(call, **expressifiable_args),
            evaluate_output_names=self._evaluate_output_names,
            alias_output_names=self._alias_output_names,
            version=self._version,
        )

    def _with_binary(self, op: Callable[..., Expression], other: Self | Any) -> Self:
        return self.__class__(
            self._callable_to_eval_series(op, other=other),
            evaluate_output_names=self._evaluate_output_names,
            alias_output_names=self._alias_output_names,
            version=self._version,
        )

    def _with_alias_output_names(self, func: AliasNames | None, /) -> Self:
        return type(self)(
            self._call,
            evaluate_output_names=self._evaluate_output_names,
            alias_output_names=func,
            version=self._version,
        )

    def __and__(self, other: Self) -> Self:
        return self._with_binary(lambda expr, other: (expr & other), other=other)

    def __or__(self, other: Self) -> Self:
        return self._with_binary(lambda expr, other: (expr | other), other=other)

    def __invert__(self) -> Self:
        invert = cast("Callable[..., Expression]", operator.invert)
        return self._with_elementwise(invert)

    def __add__(self, other) -> Self:
        return self._with_binary(lambda expr, other: (expr + other), other)

    def __sub__(self, other) -> Self:
        return self._with_binary(lambda expr, other: (expr - other), other)

    def __rsub__(self, other) -> Self:
        return self._with_binary(lambda expr, other: (other - expr), other)

    def __mul__(self, other: Self) -> Self:
        return self._with_binary(lambda expr, other: (expr * other), other)

    def __truediv__(self, other: Self) -> Self:
        return self._with_binary(lambda expr, other: (expr / other), other)

    def __rtruediv__(self, other: Self) -> Self:
        return self._with_binary(lambda expr, other: (other / expr), other)

    def __floordiv__(self, other: Self) -> Self:
        return self._with_binary(lambda expr, other: (expr / other).floor(), other)

    def __rfloordiv__(self, other: Self) -> Self:
        return self._with_binary(
            lambda expr, other: (other / expr).floor(), other
        ).alias("literal")

    def __mod__(self, other: Self) -> Self:
        return self._with_binary(lambda expr, other: (expr % other), other)

    def __rmod__(self, other: Self) -> Self:
        return self._with_binary(lambda expr, other: (other % expr), other)

    def __pow__(self, other: Self) -> Self:
        return self._with_binary(lambda expr, other: (expr**other), other)

    def __rpow__(self, other: Self) -> Self:
        return self._with_binary(lambda expr, other: (other**expr), other)

    def __gt__(self, other: Self) -> Self:
        return self._with_binary(lambda expr, other: (expr > other), other)

    def __ge__(self, other: Self) -> Self:
        return self._with_binary(lambda expr, other: (expr >= other), other)

    def __lt__(self, other: Self) -> Self:
        return self._with_binary(lambda expr, other: (expr < other), other)

    def __le__(self, other: Self) -> Self:
        return self._with_binary(lambda expr, other: (expr <= other), other)

    def __eq__(self, other: Self) -> Self:
        return self._with_binary(lambda expr, other: (expr == other), other)

    def __ne__(self, other: Self) -> Self:
        return self._with_binary(lambda expr, other: (expr != other), other)

    def over(
        self, partition_by: Sequence[str | Expression], order_by: Sequence[str]
    ) -> Self:
        def func(df: DaftLazyFrame) -> Sequence[Expression]:
            return self.window_function(df, WindowInputs(partition_by, order_by))

        return self.__class__(
            func,
            evaluate_output_names=self._evaluate_output_names,
            alias_output_names=self._alias_output_names,
            version=self._version,
        )

    def all(self) -> Self:
        def f(expr: Expression) -> Expression:
            return coalesce(expr.bool_and(), lit(True))  # noqa: FBT003

        return self._with_callable(f)

    def any(self) -> Self:
        def f(expr: Expression) -> Expression:
            return coalesce(expr.bool_or(), lit(False))  # noqa: FBT003

        return self._with_callable(f)

    def cast(self, dtype: DType | type[DType]) -> Self:
        def func(expr: Expression) -> Expression:
            native_dtype = narwhals_to_native_dtype(dtype, self._version)
            return expr.cast(native_dtype)

        return self._with_elementwise(func)

    def count(self) -> Self:
        return self._with_elementwise(lambda _input: _input.count("valid"))

    def abs(self) -> Self:
        return self._with_elementwise(lambda _input: _input.abs())

    def mean(self) -> Self:
        return self._with_callable(lambda _input: _input.mean())

    def clip(self, lower_bound: DaftExpr, upper_bound: DaftExpr) -> Self:
        return self._with_elementwise(
            lambda expr: expr.clip(lower_bound, upper_bound),
            lower_bound=lower_bound,
            upper_bound=upper_bound,
        )

    def sum(self) -> Self:
        def f(expr: Expression) -> Expression:
            return coalesce(expr.sum(), lit(0))

        return self._with_callable(f)

    def n_unique(self) -> Self:
        return self._with_callable(
            lambda _input: _input.count_distinct() + _input.is_null().bool_or()
        )

    def len(self) -> Self:
        return self._with_callable(lambda _input: _input.count("all"))

    def std(self, ddof: int) -> Self:
        def func(expr: Expression) -> Expression:
            std_pop = expr.stddev()
            if ddof == 0:
                return std_pop
            n_samples = expr.count(mode="valid")
            return std_pop * n_samples.sqrt() / (n_samples - ddof).sqrt()

        return self._with_callable(func)

    def var(self, ddof: int) -> Self:
        def func(expr: Expression) -> Expression:
            std_pop = expr.stddev()
            var_pop = std_pop * std_pop
            if ddof == 0:
                return var_pop
            n_samples = expr.count(mode="valid")
            return var_pop * n_samples / (n_samples - ddof)

        return self._with_callable(func)

    def max(self) -> Self:
        return self._with_callable(lambda _input: _input.max())

    def min(self) -> Self:
        return self._with_callable(lambda _input: _input.min())

    def null_count(self) -> Self:
        return self._with_callable(lambda _input: _input.is_null().cast("uint32").sum())

    def is_null(self) -> Self:
        return self._with_elementwise(lambda _input: _input.is_null())

    def is_nan(self) -> Self:
        return self._with_elementwise(lambda _input: _input.float.is_nan())

    def is_finite(self) -> Self:
        return self._with_elementwise(
            lambda _input: (_input > float("-inf")) & (_input < float("inf"))
        )

    def is_in(self, other: Sequence[Any]) -> Self:
        return self._with_elementwise(lambda _input: _input.is_in(other))

    def round(self, decimals: int) -> Self:
        return self._with_elementwise(lambda _input: _input.round(decimals))

    def fill_null(self, value: Self | Any, strategy: Any, limit: int | None) -> Self:
        if strategy is not None:
            msg = "todo"
            raise NotImplementedError(msg)

        return self._with_elementwise(
            lambda _input, value: _input.fill_null(value), value=value
        )

    def log(self, base: float) -> Self:
        return self._with_elementwise(lambda expr: expr.log(base=base))

    def skew(self) -> Self:
        return self._with_callable(lambda expr: expr.skew())

    @classmethod
    def _is_expr(cls, obj: Self | Any) -> TypeIs[Self]:
        return hasattr(obj, "__narwhals_expr__")

    clip_lower = not_implemented()
    clip_upper = not_implemented()
    cum_count = not_implemented()
    cum_max = not_implemented()
    cum_min = not_implemented()
    cum_prod = not_implemented()
    cum_sum = not_implemented()
    diff = not_implemented()
    drop_nulls = not_implemented()
    fill_nan = not_implemented()
    filter = not_implemented()
    ewm_mean = not_implemented()
    exp = not_implemented()
    is_first_distinct = not_implemented()
    is_last_distinct = not_implemented()
    is_unique = not_implemented()
    kurtosis = not_implemented()
    rank = not_implemented()
    map_batches = not_implemented()
    median = not_implemented()
    mode = not_implemented()
    quantile = not_implemented()
    replace_strict = not_implemented()
    rolling_max = not_implemented()
    rolling_mean = not_implemented()
    rolling_min = not_implemented()
    rolling_sum = not_implemented()
    rolling_std = not_implemented()
    rolling_var = not_implemented()
    shift = not_implemented()
    sqrt = not_implemented()
    unique = not_implemented()
    first = not_implemented()
    last = not_implemented()
    floor = not_implemented()
    ceil = not_implemented()

    # namespaces
    str = not_implemented()  # pyright: ignore[reportAssignmentType]
    dt = not_implemented()  # pyright: ignore[reportAssignmentType]
    cat = not_implemented()  # pyright: ignore[reportAssignmentType]
    list = not_implemented()  # pyright: ignore[reportAssignmentType]
    struct = not_implemented()  # pyright: ignore[reportAssignmentType]
