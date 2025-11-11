from ._rust.session import *
from ._rust.session import RequestResult, Session
from ._rust.statements import PyPreparedStatement
from ._rust.session import *
from ._rust.writers import SerializationBuffer
from typing import Any

from .serializer import SerializedValues


async def execute_unpaged_python(
        self: Session, prepared: PyPreparedStatement, values: list[Any]
) -> RequestResult:

    try:
        serialized_values = SerializedValues(prepared)
        serialized_values.add_row(values)

        data = serialized_values.get_content()

        return await self._execute_raw_bytes(prepared, data)

    except Exception as e:
        raise RuntimeError(f"Failed to serialize values: {e}") from e


Session.execute_unpaged_python= execute_unpaged_python