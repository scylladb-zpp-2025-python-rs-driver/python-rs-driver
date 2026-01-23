from .results import RequestResult
from .statement import PreparedStatement, Statement
from .types import CqlValueList

class Session:
    async def execute(
        self,
        request: PreparedStatement | Statement | str,
        values: CqlValueList | None = None,
    ) -> RequestResult: ...
    async def prepare(self, statement: Statement | str) -> PreparedStatement: ...
