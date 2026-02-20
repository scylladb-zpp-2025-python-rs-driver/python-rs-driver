from collections.abc import Iterator
from typing import Protocol, runtime_checkable

from .._rust.cluster import ClusterState, NodeShard  # pyright: ignore[reportMissingModuleSource]
from .._rust.policies.load_balancing import (  # pyright: ignore[reportMissingModuleSource]
    DefaultPolicy,
    LatencyAwareness,
    RoutingInfo,
)


@runtime_checkable
class LoadBalancingPolicy(Protocol):
    def fallback(self, routing_info: RoutingInfo, cluster_state: ClusterState) -> Iterator[NodeShard]: ...


__all__ = ["DefaultPolicy", "LatencyAwareness", "LoadBalancingPolicy", "RoutingInfo"]
