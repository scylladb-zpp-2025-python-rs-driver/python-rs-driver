from typing import Any

class Session:
    async def execute(self, request: str) -> RequestResult: ...
    async def prepare(self, request: str) -> PyPreparedStatement: ...
    async def execute_unpaged_python(
        self, prepared: PyPreparedStatement, values: list[Any]
    ) -> RequestResult: ...
    async def _execute_raw_bytes(
        self, prepared: PyPreparedStatement, bytes: bytes, elem_count: int
    ) -> RequestResult: ...

class RequestResult:
    def __str__(self) -> str: ...
    pass

class PyPreparedStatement:
    pass

class PyRowSerializationContext:
    @staticmethod
    def from_prepared(prepared: PyPreparedStatement) -> PyRowSerializationContext: ...
    def column_count(self) -> int: ...
    def columns(self) -> list[dict[str, Any]]: ...

__all__ = [
    "Session",
    "PyPreparedStatement",
    "PyRowSerializationContext",
    "RequestResult",
]
