from .results import PagingState, RequestResult, RowFactory
from typing import Any

from .statement import PreparedStatement, Statement

class Session:
    """
    Represents a CQL session, which can be used to communicate with the database
    """

    async def prepare(self, statement: Statement | str) -> PreparedStatement:
        """
        Prepare a statement for repeated execution.

        Parameters
        ----------
        statement : Statement | str
            The statement to prepare.

        Returns
        -------
        PreparedStatement
            A prepared statement ready for execution with parameters.
        """
        ...

    async def execute(
        self,
        statement: PreparedStatement | Statement | str,
        values: Any | None = None,
        factory: RowFactory| None = None,
        paging_state: PagingState | None = None,
        paged: bool = True
    ) -> RequestResult:
        """
        Execute a query and return results.

        Parameters
        ----------
        statement : PreparedStatement | Statement | str
            The statement to execute.
        values : Any | None, optional
            Query parameters to bind to the statement. Default is None.
        factory : RowFactory | None, optional
            Row factory to use for constructing row objects. If None, uses default
            dictionary mapping. Default is None.
        paging_state : PagingState | None, optional
            Paging state to resume from a previous query. Default is None.
        paged : bool, optional
            Enable automatic paging if True. Default is True.

        Returns
        -------
        RequestResult
            Query results with paging support.
        """
        ...
