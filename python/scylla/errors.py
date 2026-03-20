from __future__ import annotations
from ._rust.errors import (  # pyright: ignore[reportMissingModuleSource]
    ScyllaError,
    ConnectionError,
    SessionConfigError,
    StatementConversionError,
    PrepareError,
    ExecuteError,
    SchemaAgreementError,
    StatementConfigError,
)


__all__ = [
    "ScyllaError",
    "ConnectionError",
    "SessionConfigError",
    "StatementConversionError",
    "PrepareError",
    "ExecuteError",
    "SchemaAgreementError",
    "StatementConfigError",
]
