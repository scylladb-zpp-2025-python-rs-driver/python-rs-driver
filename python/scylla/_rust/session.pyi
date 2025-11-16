from scylla.serialize.column_type import RowSerializationContext
from scylla.serialize.serialize import ValueList

class Session:
    async def execute(self, request: str) -> RequestResult: ...
    async def prepare(self, request: str) -> PyPreparedStatement: ...
    async def _execute_raw_bytes(
        self, prepared: PyPreparedStatement, bytes: bytes, elem_count: int
    ) -> RequestResult: ...
    async def execute_with_column_spec(
        self: Session, prepared: PyPreparedStatement, values: ValueList
    ) -> RequestResult: ...

class RequestResult:
    def __str__(self) -> str: ...
    pass

class PyPreparedStatement:
    pass

class PyRowSerializationContext:
    @staticmethod
    def from_prepared(prepared: PyPreparedStatement) -> PyRowSerializationContext: ...
    def get_context(self) -> RowSerializationContext: ...
