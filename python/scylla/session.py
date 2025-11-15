from dataclasses import is_dataclass, asdict

from ._rust.session import *
from ._rust.session import RequestResult, Session
from ._rust.statements import PyPreparedStatement
from ._rust.session import *
from ._rust.writers import SerializationBuffer
from typing import Any

from .serializer import SerializedValues


def _normalize_values(prepared: PyPreparedStatement, values: Any) -> list[Any]:
    columns = prepared.get_columns_name()

    if isinstance(values, (list, tuple)):
        return list(values)

    if isinstance(values, dict):
        try:
            return [values[col] for col in columns]
        except KeyError as e:
            raise RuntimeError(f"Missing key for column {e.args[0]}") from e

    if is_dataclass(values):
        values = asdict(values)
        try:
            return [values[col] for col in columns]
        except KeyError as e:
            raise RuntimeError(f"Missing field for column {e.args[0]}") from e

    if hasattr(values, "__dict__"):
        attrs = vars(values)
        try:
            return [attrs[col] for col in columns]
        except KeyError as e:
            raise RuntimeError(f"Missing attribute for column {e.args[0]}") from e

    return [values]


async def execute_unpaged_python(
    self: Session, prepared: PyPreparedStatement, values: Any
) -> RequestResult:
    try:
        values = _normalize_values(prepared, values)
        serialized_values = SerializedValues(prepared)
        serialized_values.add_row(values)

        data = serialized_values.get_content()

        return await self._execute_raw_bytes(prepared, data)

    except Exception as e:
        raise RuntimeError(f"Failed to serialize values: {e}") from e


Session.execute_unpaged_python = execute_unpaged_python
