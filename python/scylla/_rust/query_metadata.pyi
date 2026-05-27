from __future__ import annotations

from .cluster.metadata import CqlColumnType

class ColumnSpec:
    """
    Specification of a column in a result set, used for both prepared statement metadata and query result metadata.
    """

    @property
    def name(self) -> str: ...
    @property
    def table_name(self) -> str: ...
    @property
    def keyspace_name(self) -> str: ...
    @property
    def cql_type(self) -> CqlColumnType: ...
