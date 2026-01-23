from typing import Sequence, AbstractSet, TypeAlias, Mapping
from .results import CqlNative
import ipaddress
from datetime import date, datetime, time
from decimal import Decimal
from uuid import UUID

class UnsetType:
    """
    Type of the Unset singleton.
    """

    def __repr__(self) -> str: ...
    def __str__(self) -> str: ...

CqlCollection: TypeAlias = (
    # CQL: List, Tuple, Vector
    Sequence["CqlValue"]
    # CQL: Set
    | AbstractSet["CqlValue"]
    # CQL: Map, UserDefinedType (UDT)
    | Mapping[int, "CqlValue"]
    | Mapping[float, "CqlValue"]
    | Mapping[str, "CqlValue"]
    | Mapping[bool, "CqlValue"]
    | Mapping[bytes, "CqlValue"]
    | Mapping[Decimal, "CqlValue"]
    | Mapping[UUID, "CqlValue"]
    | Mapping[ipaddress.IPv4Address, "CqlValue"]
    | Mapping[ipaddress.IPv6Address, "CqlValue"]
    | Mapping[date, "CqlValue"]
    | Mapping[datetime, "CqlValue"]
    | Mapping[time, "CqlValue"]
)

CqlValue = CqlCollection | CqlNative

CqlValueList: TypeAlias = Sequence[CqlValue] | Mapping[str, CqlValue]
