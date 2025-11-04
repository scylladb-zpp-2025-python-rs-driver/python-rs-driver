from ._rust.session import RequestResult, Session, PyPreparedStatement
from ._rust.session import *
from .serializalize import serialize as _serialize
from typing import Any


async def execute_unpaged_python(
    self: Session, prepared: PyPreparedStatement, values: list[Any]
) -> RequestResult:
    """
    Execute a prepared statement with Python serialization.

    prepared: PyPreparedStatement object
    values: List of values to serialize
    """

    try:
        # Python serializer
        serializer_result = _serialize(values, prepared)

        serialized_values, element_count = serializer_result

        # u16 range check
        if element_count < 0 or element_count > 65535:
            raise RuntimeError(
                f"Element count must be in u16 range (0-65535), got {element_count}"
            )

        # Call the Rust layer
        return await self._execute_raw_bytes(prepared, serialized_values, element_count)

    except Exception as e:
        raise RuntimeError(f"Failed to serialize values: {e}") from e


# Attach method to Session
Session.execute_unpaged_python = execute_unpaged_python
