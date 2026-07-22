from collections.abc import Iterable
from typing import Protocol, runtime_checkable

from ._rust.load_balancing import DefaultPolicy, RoutingInfo  # pyright: ignore[reportMissingModuleSource]
from .cluster import ClusterState, Node
from .routing import Shard


@runtime_checkable
class LoadBalancingPolicy(Protocol):
    """
    Represents a custom load balancing policy object implemented by the
    Python user. To define one, create a class with a pick_targets
    method that conforms to this protocol.
    """

    def pick_targets(
        self,
        routing_info: RoutingInfo,
        cluster_state: ClusterState,
    ) -> Iterable[tuple[Node, Shard | None]]:
        """
        Return an iterator of ``(Node, Shard)`` tuples that are
        the preferred targets for the given request.
        """
        ...


__all__ = ["DefaultPolicy", "LoadBalancingPolicy", "RoutingInfo"]
