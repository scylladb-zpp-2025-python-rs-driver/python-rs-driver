from typing import TypeAlias

from ._rust.routing import (  # pyright: ignore[reportMissingModuleSource]
    Token,
)

Shard: TypeAlias = int

__all__ = [
    "Shard",
    "Token",
]
