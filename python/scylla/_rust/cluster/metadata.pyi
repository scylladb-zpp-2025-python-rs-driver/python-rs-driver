from __future__ import annotations

from enum import IntEnum
from typing import Mapping

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
    def replication_factor(self) -> Mapping[str, int] | int | None:
        """
        Access the replication factor for this strategy.

        For Simple and Local strategies, this is a positive integer.
        For Network Topology strategy, this is a read-only dictionary mapping datacenter names to replication factors.
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
    def other_data(self) -> Mapping[str, str] | None:
        """
        Access the data of the strategy, if it is of the Other kind.
        """
        ...
    def __repr__(self) -> str: ...

class CqlColumnType:
    """Base class for all CQL column types (native, collections, vectors, tuples, UDTs)."""

    ...

class CqlNativeType:
    """Base class for native Cassandra scalar types."""

    ...

class CqlAscii(CqlNativeType): ...
class CqlBigInt(CqlNativeType): ...
class CqlBlob(CqlNativeType): ...
class CqlBoolean(CqlNativeType): ...
class CqlCounter(CqlNativeType): ...
class CqlDate(CqlNativeType): ...
class CqlDecimal(CqlNativeType): ...
class CqlDouble(CqlNativeType): ...
class CqlDuration(CqlNativeType): ...
class CqlFloat(CqlNativeType): ...
class CqlInt(CqlNativeType): ...
class CqlInet(CqlNativeType): ...
class CqlSmallInt(CqlNativeType): ...
class CqlText(CqlNativeType): ...
class CqlTime(CqlNativeType): ...
class CqlTimestamp(CqlNativeType): ...
class CqlTimeuuid(CqlNativeType): ...
class CqlTinyInt(CqlNativeType): ...
class CqlUuid(CqlNativeType): ...
class CqlVarint(CqlNativeType): ...

class CqlCollectionType:
    """Base class for CQL collection types (List, Map, Set)."""

    frozen: bool

class CqlList(CqlCollectionType):
    """CqlList<T> — ordered sequence of elements."""

    column_type: CqlColumnType

class CqlMap(CqlCollectionType):
    """CqlMap<K, V> — key-value pairs."""

    key_type: CqlColumnType
    value_type: CqlColumnType

class CqlSet(CqlCollectionType):
    """CqlSet<T> — unordered set of elements."""

    column_type: CqlColumnType

class CqlTuple(CqlColumnType):
    """CqlTuple<T1, T2, ...> — positional tuple of column types."""

    element_types: list[CqlColumnType]

class CqlVector(CqlColumnType):
    """CqlVector<T, N> — fixed-length vector of elements."""

    typ: CqlColumnType
    dimensions: int

class CqlUserDefinedType(CqlColumnType):
    """CQL user-defined type (UDT) — custom type with named fields."""

    name: str
    frozen: bool
    keyspace: str
    field_types: list[tuple[str, CqlColumnType]]

class ColumnKind(IntEnum):
    Regular = ...
    Static = ...
    Clustering = ...
    PartitionKey = ...

class Column:
    @property
    def typ(self) -> CqlColumnType:
        """
        Access the type of this column.
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
    def columns(self) -> Mapping[str, Column]:
        """
        Access the columns of this table as a read-only dictionary of name to column.
        """
        ...
    @property
    def partition_key(self) -> Mapping[str, Column]:
        """
        Access the partition key of this table as a read-only dictionary of name to column.
        """
        ...
    @property
    def clustering_key(self) -> Mapping[str, Column]:
        """
        Access the clustering key of this table as a read-only dictionary of name to column.
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
    def partition_key(self) -> Mapping[str, Column]:
        """
        Access the partition key of this view as a read-only dictionary of name to column.
        """
        ...
    @property
    def clustering_key(self) -> Mapping[str, Column]:
        """
        Access the clustering key of this view as a read-only dictionary of name to column.
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
    def tables(self) -> Mapping[str, Table]:
        """
        Access the tables of this keyspace as a read-only dictionary of name to table.
        """
        ...
    @property
    def views(self) -> Mapping[str, MaterializedView]:
        """
        Access the materialized views of this keyspace as a read-only dictionary of name to view.
        """
        ...
    def __repr__(self) -> str: ...
