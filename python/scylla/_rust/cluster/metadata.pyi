from __future__ import annotations

from enum import IntEnum

class StrategyKind(IntEnum):
    Simple = ...
    NetworkTopology = ...
    Local = ...
    Other = ...

class Strategy:
    @property
    def kind(self) -> StrategyKind:
        """
        Access the kind of this strategy.
        """
        ...
    @property
    def replication_factor(self) -> dict[str, int] | int | None:
        """
        Access the replication factor for this strategy.

        For Simple and Local strategies, this is a positive integer.
        For Network Topology strategy, this is a dictionary mapping datacenter names to replication factors.
        None means Driver cannot determine the replication factor based on Strategy.
        """
        ...
    @property
    def other_name(self) -> str | None:
        """
        Access the name of the strategy, if it is of the Other kind.
        """
        ...
    @property
    def other_data(self) -> dict[str, str] | None:
        """
        Access the data of the strategy, if it is of the Other kind.
        """
        ...
    def __repr__(self) -> str: ...

class ColumnKind(IntEnum):
    Regular = ...
    Static = ...
    Clustering = ...
    PartitionKey = ...

class Column:
    @property
    def typ(self) -> str:
        """
        Access the type of this column as a string.
        """
        ...
    @property
    def kind(self) -> ColumnKind:
        """
        Access the kind of this column.
        """
        ...
    def __repr__(self) -> str: ...

class Table:
    @property
    def columns(self) -> dict[str, Column]:
        """
        Access the columns of this table as a dictionary of name to column.
        """
        ...
    @property
    def partition_key(self) -> dict[str, Column]:
        """
        Access the partition key of this table as a dictionary of name to column.
        """
        ...
    @property
    def clustering_key(self) -> dict[str, Column]:
        """
        Access the clustering key of this table as a dictionary of name to column.
        """
        ...
    @property
    def partitioner(self) -> str | None:
        """
        Access the name of partitioner used by this table or None.
        """
        ...
    def __repr__(self) -> str: ...

class MaterializedView:
    @property
    def base_table_name(self) -> str:
        """
        Access the name of the base table of this materialized view.
        """
        ...
    @property
    def columns(self) -> dict[str, Column]:
        """
        Access the columns of this materialized view as a dictionary of name to column.
        """
        ...
    @property
    def partition_key(self) -> list[str]:
        """
        Access the partition key of this materialized view as a list of column names.
        """
        ...
    @property
    def clustering_key(self) -> list[str]:
        """
        Access the clustering key of this materialized view as a list of column names.
        """
        ...
    @property
    def partitioner(self) -> str | None:
        """
        Access the name of partitioner used by this materialized view or None.
        """
        ...
    def __repr__(self) -> str: ...

class Keyspace:
    @property
    def strategy(self) -> Strategy:
        """
        Access the strategy used by this keyspace.
        """
        ...
    @property
    def tables(self) -> dict[str, Table]:
        """
        Access the tables of this keyspace as a dictionary of name to table.
        """
        ...
    @property
    def views(self) -> dict[str, MaterializedView]:
        """
        Access the materialized views of this keyspace as a dictionary of name to view.
        """
        ...
    def __repr__(self) -> str: ...
