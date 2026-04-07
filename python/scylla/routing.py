from typing import TypeAlias

from ._rust.routing import (  # pyright: ignore[reportMissingModuleSource]
    ReplicaLocator,
    Token,
)

Shard: TypeAlias = int

__all__ = [
    "ReplicaLocator",
    "Shard",
    "Token",
]
