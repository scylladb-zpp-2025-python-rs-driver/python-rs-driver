from scylla.serialize.column_type import RowSerializationContext
from scylla.serialize.serialize import ValueList

from ._rust.session import (
    PyPreparedStatement,
    PyRowSerializationContext,
    RequestResult,
    Session,
)

__all__ = ["Session"]


async def execute_with_column_spec(
    self: Session, prepared: PyPreparedStatement, values: ValueList
) -> RequestResult:
    py_ctx = PyRowSerializationContext.from_prepared(prepared)
    row_ctx: RowSerializationContext = py_ctx.get_context()

    try:
        # Serialize all values with their column types
        serialized_data, element_count = values.serialize(row_ctx)
        # Call the Rust layer
        return await self._execute_raw_bytes(
            prepared, bytes(serialized_data), element_count
        )

    except Exception as e:
        raise RuntimeError(f"Failed to serialize and execute: {e}") from e


# Attach method to Session
Session.execute_with_column_spec = execute_with_column_spec
