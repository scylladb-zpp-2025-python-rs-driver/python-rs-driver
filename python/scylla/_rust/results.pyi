import ipaddress
from datetime import date, datetime, time
from decimal import Decimal
from typing import Dict, List, Union, Tuple, Any, Set, AsyncIterator
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

class SinglePageIterator:
    """
    Iterator over result rows.

    Each iteration yields a single row object, by default a dictionary
    mapping column names to values.
    """

    def __iter__(self) -> SinglePageIterator: ...
    def __next__(self) -> Any: ...

class PagingState:
    """
    Represents paging state for paged queries.

    Used to continue a query from where the previous page ended.
    """

    def __init__(self) -> None:
        """
        Creates a new paging state starting from the first page.
        """
        ...

class RequestResult:
    """
    Result of a query execution.
    """

    def has_more_pages(self) -> bool:
        """
        Returns True if more pages are available.
        """
        ...

    def paging_state(self) -> PagingState | None:
        """
        Returns current paging state. Can be `None` if there are no more pages available.
        """
        ...

    async def fetch_next_page(self) -> None:
        """
        Fetches the next page and updates the internal query result.
        No-op if no more pages are available.
        """
        ...

    def iter_page(
        self,
        factory: RowFactory | None = None,
    ) -> SinglePageIterator:
        """
        Returns an iterator over rows in the current page.

        If a factory is provided, it is used to construct row objects
        for this iterator only.
        """
        ...

    def set_factory(self, factory: RowFactory) -> None:
        """
        Sets the default row factory used to construct row objects.

        The factory will be used by subsequent calls to `iter_page()`
        and by async iteration (`async for`).

        Parameters
        ----------
        factory : RowFactory
            Row factory used to build row objects from column iterators.
        """
        ...

    def __aiter__(self) -> AsyncRowsIterator: ...

class AsyncRowsIterator(AsyncIterator[Any]):
    """
    Async iterator over rows.
    """

    def __aiter__(self) -> AsyncRowsIterator: ...
    async def __anext__(self) -> Any: ...
