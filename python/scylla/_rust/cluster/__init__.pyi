from __future__ import annotations

from ipaddress import IPv4Address, IPv6Address
from types import MappingProxyType
from uuid import UUID

from ..routing import Shard, Token
from .metadata import Keyspace

class NodeShard:
    """
    Represents a node with a specified shard.
    """

    def __init__(self, host_id: UUID, shard: Shard | None) -> None: ...
    @property
    def host_id(self) -> UUID:
        """
        Access the host ID of the node.
        """
        ...
    @property
    def shard(self) -> Shard | None:
        """
        Access the shard of the node.
        """
        ...
    def __repr__(self) -> str: ...

class Node:
    """
    Represents a node in the cluster.
    """

    @property
    def host_id(self) -> UUID:
        """
        Access the host ID of the node.
        """
        ...
    @property
    def address(self) -> tuple[IPv4Address | IPv6Address, int]:
        """
        Returns a tuple of (`IPv4Address` | `IPv6Address`, `int`) representing address and port of a Node.
        """
        ...
    @property
    def datacenter(self) -> str | None:
        """
        Access the datacenter of the node.
        """
        ...
    @property
    def rack(self) -> str | None:
        """
        Access the rack of the node.
        """
        ...
    @property
    def nr_shards(self) -> int | None:
        """
        Access the number of shards of the node.
        """
        ...
    @property
    def connected(self) -> bool:
        """
        Is `True` if the driver has a connection open to this node,
        `False` otherwise.
        """
        ...
    @property
    def enabled(self) -> bool:
        """
        Is `True` if the node is enabled, `False` otherwise.

        Only enabled nodes will have connections open.
        """
        ...
    def __repr__(self) -> str: ...

class ClusterState:
    """
    Represents state of the cluster allowing access to known nodes,
    keyspaces and token calculation.

    """

    def get_keyspace(self, keyspace: str) -> Keyspace | None:
        """
        Get the keyspace by name.
        """
        ...
    @property
    def keyspaces(self) -> MappingProxyType[str, Keyspace]:
        """
        Access the keyspaces of the cluster as a dictionary of
        keyspace name to Keyspace object.
        """
        ...
    @property
    def nodes_info(self) -> MappingProxyType[UUID, Node]:
        """
        Access the nodes of the cluster as a dict of host ID to Node objects.
        """
        ...
    def compute_token(self, keyspace: str, table: str, partition_key: object) -> Token:
        """
        Computes the token for a given keyspace, table and partition key.

        `partition_key` must be a `Sequence` of partition key values or a
        `Mapping` of column names to partition key values.
        """
        ...
    def get_token_endpoints(self, keyspace: str, table: str, token: Token) -> list[tuple[Node, Shard]]:
        """
        Returns a list of `[Node, Shard]` tuples that are considered
        to be replicas for a given token.
        """
        ...
    def get_endpoints(self, keyspace: str, table: str, partition_key: object) -> list[tuple[Node, Shard]]:
        """
        Returns a list of `[Node, Shard]` tuples that are replicas owning the partition key.

        `partition_key` must be a `Sequence` of partition key values or a
        `Mapping` of column names to partition key values.
        """
        ...
    def __repr__(self) -> str: ...
