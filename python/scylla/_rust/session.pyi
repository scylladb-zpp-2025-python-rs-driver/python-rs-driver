from scylla.serialize.column_type import RowSerializationContext

class Session:
    async def execute(self, request: str) -> RequestResult: ...

class RequestResult:
    def __str__(self) -> str: ...

class PyPreparedStatement:
    pass

class PyRowSerializationContext:
    @staticmethod
    def from_prepared(prepared: PyPreparedStatement) -> PyRowSerializationContext: ...
    def get_context(self) -> RowSerializationContext: ...
