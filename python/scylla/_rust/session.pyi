from typing import Optional

from .results import RequestResult, PagingState, PagingRequestResult, AsyncRowsIterator
from .statement import PreparedStatement, Statement

class Session:
    async def execute(self, request: PreparedStatement | Statement | str) -> RequestResult: ...
    async def prepare(self, statement: Statement | str) -> PreparedStatement: ...
    async def execute_paged(
        self,
        request: PreparedStatement | str,
        paging_state: Optional[PagingState] = None,
        page_size: Optional[int] = None,
    ) -> PagingRequestResult: ...
    async def execute_async_paged(
        self,
        request: PreparedStatement | Statement | str,
        page_size: Optional[int] = None,
    ) -> AsyncRowsIterator: ...
