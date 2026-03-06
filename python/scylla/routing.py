from typing import TypeAlias

from ._rust.routing import (  # pyright: ignore[reportMissingModuleSource]
    ReplicaLocator,
    ReplicaSet,
    ReplicaSetIterator,
    Sharder,
    Token,
)

Shard: TypeAlias = int

__all__ = [
    "ReplicaLocator",
    "ReplicaSet",
    "ReplicaSetIterator",
    "Shard",
    "Sharder",
    "Token",
]
