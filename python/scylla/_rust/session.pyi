from typing import Any
import uuid

from .batch import Batch
from .cluster import ClusterState
from .results import PagingState, RequestResult, RowFactory
from .statement import PreparedStatement, Statement

class Session:
    """
    Represents a CQL session, which can be used to communicate with the database.
    """

    @property
    def cluster_state(self) -> ClusterState:
        """
        Access information about the cluster topology or schema through ClusterState object.
        """
        ...
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
        /,
        *,
        factory: RowFactory | None = None,
        paging_state: PagingState | None = None,
        paged: bool = True,
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
            Enable automatic paging if True. Otherwise, all rows come in a single result frame,
            which is **strongly discouraged** for large (over thousands of rows) responses,
            and acceptable for responses containing few or no rows.
            Default is True.

        Returns
        -------
        RequestResult
            Query results with paging support.
        """
        ...

    async def batch(
        self,
        batch: Batch,
        /,
        *,
        factory: RowFactory | None = None,
    ) -> RequestResult:
        """
        Execute a batch statement, which can contain many `Statement`s and `PreparedStatement`s.

        Parameters
        ----------
        batch : Batch
            The batch of statements and their values to execute.

        Returns
        -------
        RequestResult
            For non-LWT batches, the result does not contain rows.
            For LWT batches, the result contains rows with a boolean `[applied]` column.
            In each returned row, columns other than `[applied]` contain either the current
            values of that row (if the condition was not met) or `None` values (if it was met).

        """
        ...

    async def await_schema_agreement(self) -> uuid.UUID:
        """
        Wait until all nodes in the cluster agree on the current schema version.

        This is useful after performing schema-altering operations to ensure that all nodes
        have updated their schema before proceeding with operations that depend on the new schema.

        Returns
        -------
        uuid.UUID
            The agreed schema version as a UUID object.

        Raises
        ------
        RuntimeError
            If the schema agreement could not be reached.
        """
        ...

    async def check_schema_agreement(self) -> uuid.UUID | None:
        """
        Check if all nodes in the cluster agree on the current schema version.

        Unlike `await_schema_agreement`, this method does not wait for agreement to be reached,
        but instead returns the current state immediately.

        Returns
        -------
        uuid.UUID | None
            The agreed schema version as a UUID object if all nodes agree, None otherwise.

        Raises
        ------
        RuntimeError
            If the schema agreement check failed.
        """
        ...
