from enum import IntEnum
from datetime import timedelta

from typing import Literal

class Consistency(IntEnum):
    Any = ...
    One = ...
    Two = ...
    Three = ...
    Quorum = ...
    All = ...
    LocalQuorum = ...
    EachQuorum = ...
    LocalOne = ...
    Serial = ...
    LocalSerial = ...

class SerialConsistency(IntEnum):
    Serial = ...
    LocalSerial = ...

class Compression(IntEnum):
    Lz4 = ...
    Snappy = ...

class PoolSize:
    @staticmethod
    def per_host(connections: int) -> PoolSize:
        """
        Creates a pool size with a fixed number of connections per node.

        Parameters
        ----------
        connections : int
            Number of connections per node. Must be greater than 0.

        Returns
        -------
        PoolSize
        """
        ...

    @staticmethod
    def per_shard(connections: int) -> PoolSize:
        """
        Creates a pool size with a fixed number of connections per shard.

        For Cassandra, nodes are treated as having a single shard.

        The recommended setting for Scylla is ``per_shard(1)``.

        Parameters
        ----------
        connections : int
            Number of connections per shard. Must be greater than 0.

        Returns
        -------
        PoolSize
        """
        ...

    @property
    def kind(self) -> Literal["per_host", "per_shard"]:
        """
        The kind of the connection pool configuration.
        """
        ...

    @property
    def connections(self) -> int:
        """
        The underlying number of configured connections.
        """
        ...

    def __repr__(self) -> str: ...

