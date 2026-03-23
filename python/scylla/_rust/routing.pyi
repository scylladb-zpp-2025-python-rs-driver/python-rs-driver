from __future__ import annotations

from typing import TypeAlias

from .cluster import Node
from .cluster.metadata import Strategy

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

class ReplicaLocator:
    """
    `ReplicaLocator` provides a way to find the set of owning nodes
    for a given (token, replication strategy, table) tuple.
    """
    def primary_replica_for_token(
        self,
        token: Token,
        strategy: Strategy,
        keyspace: str,
        table: str,
        datacenter: str | None = None,
    ) -> tuple[Node, Shard] | None:
        """
        Returns a `[Node, Shard]` tuple that is
        considered to be a primary replica for a given token, strategy
        and table.

        This method should be preferred over `all_replicas_for_token()`
        due to performance cost that comes with materializing all replicas.

        If the `datacenter` parameter is not None, the returned
        replica is limited only to replicas from that datacenter.

        If a specified datacenter name does not correspond to a valid
        datacenter, or if there are no replicas, None will be returned.
        """
        ...

    def all_replicas_for_token(
        self,
        token: Token,
        strategy: Strategy,
        keyspace: str,
        table: str,
        datacenter: str | None = None,
    ) -> list[tuple[Node, Shard]]:
        """
        Returns a list of `[Node, Shard]` tuples that are
        considered to be replicas for a given token, strategy
        and table.

        If the `datacenter` parameter is not None, the returned
        list is limited only to replicas from that datacenter.
        If a specified datacenter name does not correspond to a valid
        datacenter, an empty list will be returned.
        """
        ...
    def unique_token_owning_nodes_in_cluster(self) -> list[Node]:
        """
        Returns a list of all nodes that own tokens in the cluster.
        """
        ...
    def unique_token_owning_nodes_in_datacenter(self, datacenter: str) -> list[Node] | None:
        """
        Returns a list of all nodes that own tokens in the datacenter
        or `None` when datacenter is not found.
        """
        ...
    @property
    def datacenter_names(self) -> list[str]:
        """
        Returns a list of all datacenter names in the cluster.
        """
        ...
    def __repr__(self) -> str: ...
