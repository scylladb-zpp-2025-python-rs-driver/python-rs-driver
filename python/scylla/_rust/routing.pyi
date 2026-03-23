from __future__ import annotations

from typing import TypeAlias

Shard: TypeAlias = int
"""`int` that fits in 32 bit unsigned integer representing Node's Shard."""

class Token:
    """
    Token is a result of computing a hash of a primary key.
    """
    def __init__(self, value: int) -> None: ...
    @property
    def value(self) -> int: ...
    def __eq__(self, other: object) -> bool: ...
    def __hash__(self) -> int: ...
    def __repr__(self) -> str: ...
