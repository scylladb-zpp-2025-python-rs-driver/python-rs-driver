import uuid
from collections.abc import Iterator

import pytest
from scylla.cluster import ClusterState, NodeShard
from scylla.enums import Consistency, SerialConsistency
from scylla.execution_profile import ExecutionProfile
from scylla.policies.load_balancing import DefaultPolicy, LatencyAwareness, LoadBalancingPolicy, RoutingInfo


def test_node_shard() -> None:
    h = uuid.uuid4()
    ns = NodeShard(h, 3)
    assert ns.host_id == h and ns.shard == 3 and NodeShard(h, None).shard is None
    with pytest.raises(Exception):
        NodeShard("bad", None)  # type: ignore[arg-type]
    with pytest.raises(Exception):
        NodeShard(None, None)  # type: ignore[arg-type]


def test_latency_awareness_and_default_policy() -> None:
    assert isinstance(LatencyAwareness(), LatencyAwareness)
    la = LatencyAwareness(
        exclusion_threshold=5.0, retry_period_secs=30.0, update_rate_secs=1.0, minimum_measurements=200, scale_secs=2.0
    )
    assert isinstance(la, LatencyAwareness)
    assert isinstance(DefaultPolicy(), DefaultPolicy)
    p = DefaultPolicy(
        preferred_datacenter="dc1",
        token_aware=True,
        permit_dc_failover=True,
        latency_awareness=LatencyAwareness(exclusion_threshold=3.0),
        enable_shuffling_replicas=False,
    )
    assert isinstance(p, DefaultPolicy) and callable(p.fallback)
    assert isinstance(DefaultPolicy(preferred_datacenter_and_rack=("dc2", "r3"), token_aware=False), DefaultPolicy)


def test_protocol_conformance() -> None:
    class Good:
        def fallback(self, routing_info: RoutingInfo, cluster_state: ClusterState) -> Iterator[NodeShard]:
            return iter([])

    class Bad:
        pass

    class Child(Good):
        pass

    assert isinstance(Good(), LoadBalancingPolicy)
    assert isinstance(Child(), LoadBalancingPolicy)
    assert isinstance(DefaultPolicy(), LoadBalancingPolicy)
    assert not isinstance(Bad(), LoadBalancingPolicy)
    assert not isinstance(None, LoadBalancingPolicy)


def test_custom_policy_and_execution_profile() -> None:
    hosts = [uuid.uuid4() for _ in range(3)]

    class RR:
        def __init__(self) -> None:
            self.idx = 0

        def fallback(self, routing_info: object, cluster_state: object) -> Iterator[NodeShard]:
            order = [NodeShard(hosts[(self.idx + i) % len(hosts)], None) for i in range(len(hosts))]
            self.idx = (self.idx + 1) % len(hosts)
            return iter(order)

    rr = RR()
    assert list(rr.fallback(None, None))[0].host_id == hosts[0]
    assert list(rr.fallback(None, None))[0].host_id == hosts[1]

    class Custom:
        def fallback(self, routing_info: RoutingInfo, cluster_state: ClusterState) -> Iterator[NodeShard]:
            return iter([])

    assert list(Custom().fallback(None, None)) == []  # type: ignore[arg-type]
    p = DefaultPolicy(preferred_datacenter="dc1", latency_awareness=LatencyAwareness(exclusion_threshold=3.0))
    prof = ExecutionProfile(
        policy=p,
        timeout=10.0,
        consistency=Consistency.Quorum,
        serial_consistency=SerialConsistency.LocalSerial,
    )
    assert prof.get_request_timeout() == 10.0 and prof.get_consistency() == Consistency.Quorum
    assert prof.get_serial_consistency() == SerialConsistency.LocalSerial
    assert isinstance(ExecutionProfile(policy=Custom()), ExecutionProfile)
    assert isinstance(ExecutionProfile(policy=None), ExecutionProfile) and isinstance(
        ExecutionProfile(), ExecutionProfile
    )
