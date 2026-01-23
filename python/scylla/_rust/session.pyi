from .results import PagingState, RequestResult
from typing import Any

from .results import RequestResult
from .statement import PreparedStatement, Statement

class Session:
    async def prepare(self, statement: Statement | str) -> PreparedStatement: ...
    async def execute(
        self,
        request: PreparedStatement | Statement | str,
        values: Any | None = None,
        paging_state: PagingState | None = None,
        paged: bool = True
    ) -> RequestResult: ...
