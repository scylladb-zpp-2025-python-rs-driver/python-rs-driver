from collections.abc import Iterator
from typing import Protocol, runtime_checkable

from ..cluster import ClusterState, NodeShard
from ..enums import Consistency, SerialConsistency
from ..routing import Token

@runtime_checkable
class LoadBalancingPolicy(Protocol):
    def fallback(self, routing_info: RoutingInfo, cluster_state: ClusterState) -> Iterator[NodeShard]: ...

class LatencyAwareness:
    def __init__(
        self,
        exclusion_threshold: float = 2.0,
        retry_period_secs: float = 10.0,
        update_rate_secs: float = 0.1,
        minimum_measurements: int = 50,
        scale_secs: float = 0.1,
    ) -> None: ...

class RoutingInfo:
    @property
    def consistency(self) -> Consistency: ...
    @property
    def serial_consistency(self) -> SerialConsistency | None: ...
    @property
    def token(self) -> Token | None: ...
    @property
    def table(self) -> tuple[str, str] | None: ...
    @property
    def is_confirmed_lwt(self) -> bool: ...

class DefaultPolicy:
    def __init__(
        self,
        preferred_datacenter: str | None = None,
        preferred_datacenter_and_rack: tuple[str, str] | None = None,
        token_aware: bool | None = None,
        permit_dc_failover: bool | None = None,
        latency_awareness: LatencyAwareness | None = None,
        enable_shuffling_replicas: bool | None = None,
    ) -> None: ...
    def fallback(self, routing_info: RoutingInfo, cluster_state: ClusterState) -> Iterator[NodeShard]: ...
