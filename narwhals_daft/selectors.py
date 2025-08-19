# from __future__ import annotations

# from typing import TYPE_CHECKING

# from narwhals._compliant import CompliantSelector, LazySelectorNamespace
# from narwhals_daft.expr import DaftExpr

# if TYPE_CHECKING:
#     # not sure what this should be
#     from sqlframe.base.column import Column

#     from narwhals_daft.dataframe import DaftLazyFrame
#     # check I've created this correctly
#     from narwhals_daft.expr import DaftWindowFunction

# # not sure about Column here!
# class DaftSelectorNamespace(LazySelectorNamespace["DaftLazyFrame", "Column"]):
#     @property
#     def _selector(self) -> type[DaftSelector]:
#         return DaftSelector


# class DaftSelector: ...
