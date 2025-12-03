from typing import Dict, List, Union, Tuple, Any

CqlNative = Union[
    int,
    float,
    str,
    bool,
    bytes,
    None,
]

CqlCollection = Union[
    List["CqlValue"],
    Tuple["CqlValue", ...],
    Dict["CqlValue", "CqlValue"],
]

CqlValue = Union[
    CqlNative,
    CqlCollection,
]

class ColumnIterator:
    def __iter__(self) -> ColumnIterator: ...
    def __next__(self) -> Column: ...

class RowFactory:
    def __init__(self) -> None: ...
    def build(self, column_iterator: ColumnIterator) -> Dict[str, CqlValue]: ...

class Column:
    @property
    def column_name(self) -> str: ...
    @property
    def value(self) -> CqlValue: ...

class RowsResult:
    def __next__(self) -> Any: ...
    def __iter__(self) -> RowsResult: ...

class RequestResult:
    def __str__(self) -> str: ...
    def create_rows_result(self, factory: RowFactory | None = None) -> RowsResult: ...
