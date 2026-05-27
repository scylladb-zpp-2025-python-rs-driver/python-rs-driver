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

class PartitionKeyIndex:
    """
    Specification of a partition key index in prepared statement metadata.
    """

    @property
    def index(self) -> int: ...
    @property
    def sequence_number(self) -> int: ...

class PreparedMetadata:
    """
    Metadata for a prepared statement, including column specifications and partition key indexes.
    """

    @property
    def columns(self) -> list[ColumnSpec]: ...
    @property
    def partition_key_indexes(self) -> list[PartitionKeyIndex]: ...
