from typing import Any, Tuple
class PyNativeType:
    def __init__(self) -> None: ...

class Int(PyNativeType): ...
class Float(PyNativeType): ...
class Double(PyNativeType): ...
class Text(PyNativeType): ...
class Boolean(PyNativeType):...
class BigInt(PyNativeType):...

class PyCollectionType:
    frozen: bool


class Map(PyCollectionType):
    key_type: Any
    value_type: Any


class Set(PyCollectionType):
    column_type: Any


class List(PyCollectionType):
    column_type: Any

class PyTuple:
    element_types: List[Any]

class PyUserDefinedType:
    name: str
    frozen: bool
    keyspace: str
    field_types: List[Tuple[str, Any]]
