from typing import Any

from .results import RequestResult
from .statement import PreparedStatement, Statement

class Session:
    async def execute(
        self,
        request: PreparedStatement | Statement | str,
        values: Any | None = None,
    ) -> RequestResult: ...
    async def prepare(self, statement: Statement | str) -> PreparedStatement: ...
