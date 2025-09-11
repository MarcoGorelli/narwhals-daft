from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from narwhals_daft.namespace import DaftNamespace

    from narwhals.utils import Version


def __narwhals_namespace__(version: Version) -> DaftNamespace:  # noqa: N807
    from narwhals_daft.namespace import DaftNamespace

    return DaftNamespace(version=version)


NATIVE_PACKAGE = "daft"
