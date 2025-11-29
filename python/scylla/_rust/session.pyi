from .results import RequestResult
from .statement import PreparedStatement, Statement
from typing import Any, Optional

class Session:
    async def execute(
        self, request: PreparedStatement | Statement | str, values: Optional[Any] = None
    ) -> RequestResult: ...
    async def prepare(self, statement: Statement | str) -> PreparedStatement: ...
