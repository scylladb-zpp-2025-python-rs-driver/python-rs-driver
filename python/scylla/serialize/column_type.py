from __future__ import annotations

from abc import ABC
from dataclasses import dataclass
from enum import Enum


class NativeType(Enum):
    INT = "int"
    BIGINT = "bigint"
    DOUBLE = "double"
    BOOLEAN = "boolean"
    TEXT = "text"


class ColumnType(ABC):
    pass


@dataclass(frozen=True)
class Native(ColumnType):
    type: NativeType


@dataclass(frozen=True)
class Collection(ColumnType):
    frozen: bool


@dataclass(frozen=True)
class List(Collection):
    element_type: "ColumnType"


@dataclass
class UserDefinedTypeDefinition:
    name: str
    keyspace: str
    field_types: list[tuple[str, ColumnType]]


@dataclass(frozen=True)
class UserDefinedType:
    frozen: bool
    definition: UserDefinedTypeDefinition


@dataclass(frozen=True)
class TableSpec:
    ks_name: str
    table_name: str


@dataclass(frozen=True)
class ColumnSpec:
    table_spec: TableSpec
    name: str
    typ: ColumnType


@dataclass
class RowSerializationContext:
    columns: list[ColumnSpec]
