# python/scylla/errors.py
from ._rust.errors import (
    ScyllaError,
    ExecutionError,
    BadQueryError,
    RuntimeError,
    ConnectionError,
    DeserializationError,
    UnsupportedTypeError,
    DecodeFailedError,
    PyConversionFailedError,
    InternalError,
)

__all__ = [
    "ScyllaError",
    "ExecutionError",
    "BadQueryError",
    "RuntimeError",
    "ConnectionError",
    "DeserializationError",
    "UnsupportedTypeError",
    "DecodeFailedError",
    "PyConversionFailedError",
    "InternalError",
]
