from typing import Any

from .statements import PyPreparedStatement
from .writers import SerializationBuffer

class Session:
    async def execute(self, request: str) -> RequestResult: ...
    async def prepare(self, request: str) -> PyPreparedStatement: ...
    async def execute_unpaged_python(
        self, prepared: PyPreparedStatement, values: list[Any]
    ) -> RequestResult: ...
    async def _execute_raw_bytes(
        self, prepared: PyPreparedStatement, buffer: SerializationBuffer
    ) -> RequestResult: ...

class RequestResult:
    def __str__(self) -> str: ...
