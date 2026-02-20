from typing import TypeAlias

from ._rust.routing import Sharder, Token  # pyright: ignore[reportMissingModuleSource]

Shard: TypeAlias = int

__all__ = ["Shard", "Sharder", "Token"]
