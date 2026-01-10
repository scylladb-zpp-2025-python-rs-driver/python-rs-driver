import ipaddress
from datetime import date, datetime, time
from decimal import Decimal
from typing import Dict, List, Union, Tuple, Any, Set, Optional
from uuid import UUID

from dateutil.relativedelta import relativedelta

CqlNative = Union[
    # CQL:
    # - Counter
    # - TinyInt
    # - SmallInt
    # - Int
    # - BigInt
    # - Varint
    int,
    # CQL:
    # - Float
    # - Double
    float,
    # CQL:
    # - Ascii
    # - Text
    str,
    # CQL:
    # - Boolean
    bool,
    # CQL:
    # - Blob
    bytes,
    # CQL:
    # - Decimal
    Decimal,
    # CQL:
    # - Uuid
    # - Timeuuid
    UUID,
    # CQL:
    # - Inet (IPv4)
    ipaddress.IPv4Address,
    # CQL:
    # - Inet (IPv6)
    ipaddress.IPv6Address,
    # CQL:
    # - Date
    date,
    # CQL:
    # - Timestamp
    datetime,
    # CQL:
    # - Time
    time,
    # CQL:
    # - Duration
    relativedelta,
    # CQL:
    # - Empty
    # - null
    None,
]

CqlCollection = Union[
    # CQL:
    # - List
    # - Vector
    List["CqlValue"],
    # CQL:
    # - Set
    Set["CqlValue"],
    # CQL:
    # - Tuple
    Tuple["CqlValue", ...],
    # CQL:
    # - Map
    # - UserDefinedType (UDT)
    Dict["CqlValue", "CqlValue"],
]

CqlValue = Union[
    CqlNative,
    CqlCollection,
]

class ColumnIterator:
    """
    Iterator over columns of a single row.

    Yields Column objects representing individual column values
    in the current row.
    """
    def __iter__(self) -> ColumnIterator: ...
    def __next__(self) -> Column: ...

class RowFactory:
    """
    Factory used to construct a row object from a column iterator.

    Allows custom row representations (e.g. dicts, dataclasses).
    """

    def __init__(self) -> None: ...

    def build(self, column_iterator: ColumnIterator) -> Dict[str, CqlValue]:
        """
        Build a row object from the provided column iterator.
        """
        ...

class Column:
    """
    Represents a single column in a result row.
    """
    @property
    def column_name(self) -> str:
        """Name of the column."""
        ...

    @property
    def value(self) -> CqlValue:
        """Deserialized value of the column."""
        ...

class RowsIterator:
    """
    Iterator over result rows.

    Each iteration yields a single row object, by default a dictionary
    mapping column names to values.
    """

    def __iter__(self) -> RowsIterator: ...
    def __next__(self) -> Any: ...

class RequestResult:
    """
    Result of a query execution.
    """

    def iter_rows(self, factory: Optional[RowFactory] = None) -> RowsIterator:
        """
        Return an iterator over result rows.

        An optional RowFactory can be provided to customize how rows
        are constructed.
        """
        ...
