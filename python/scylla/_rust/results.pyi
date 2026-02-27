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
    Iterates over rows in a single page of query results.

    Yields deserialized rows materialized using a `RowFactory`.
    Does not fetch additional pages - use AsyncRowsIterator for automatic paging.
    """

    def __iter__(self) -> SinglePageIterator: ...
    def __next__(self) -> Any: ...

class PagingState:
    """
    Represents paging state for paged queries.

    Used to continue a query from where the previous page ended.
    Can be passed to execute() to resume paging from a specific position.
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

    async def fetch_next_page(self) -> RequestResult | None:
        """
        Fetches the next page if available.

        Returns a new RequestResult with the next page's data if more pages
        are available. Returns None if no more pages exist.

        Returns
        -------
        RequestResult | None
            A new RequestResult with the next page data, or None if no more pages.
        """
        ...

    def iter_current_page(self) -> SinglePageIterator:
        """
        Returns an iterator over rows in the current page.
        """
        ...

    def __aiter__(self) -> AsyncRowsIterator: ...

class AsyncRowsIterator(AsyncIterator[Any]):
    """
    Async iterator over rows with automatic paging.

    Transparently fetches subsequent pages as iteration progresses.
    When the current page is exhausted, automatically retrieves the next page.
    """

    def __aiter__(self) -> AsyncRowsIterator: ...
    async def __anext__(self) -> Any: ...
