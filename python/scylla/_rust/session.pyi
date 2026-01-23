from typing import Optional

from .results import PagingState, RequestResult
from .statement import PreparedStatement, Statement

class Session:
    async def prepare(self, statement: Statement | str) -> PreparedStatement: ...
    async def execute(
        self,
        request: PreparedStatement | Statement | str,
        paging_state: PagingState | None = None,
        paged: bool = True
    ) -> RequestResult: ...
