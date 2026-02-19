from __future__ import annotations

from collections.abc import Iterator
from typing import Any

import pytest
from scylla.cluster import ClusterState, NodeShard
from scylla.execution_profile import ExecutionProfile
from scylla.policies.load_balancing import DefaultPolicy, LatencyAwareness, RoutingInfo
from scylla.session_builder import SessionBuilder


class TrackingPolicy:
    def __init__(self) -> None:
        self.call_count: int = 0
        self.nodes_seen: list[dict[str, Any]] = []

    def fallback(self, routing_info: RoutingInfo, cluster_state: ClusterState) -> Iterator[NodeShard]:
        self.call_count += 1
        nodes = cluster_state.get_nodes_info()
        for n in nodes:
            self.nodes_seen.append({"host_id": n.host_id, "address": n.address, "dc": n.datacenter})
        return iter([n.node_shard for n in nodes])


def test_invalid_latency_awareness_construction() -> None:
    with pytest.raises(ValueError):
        LatencyAwareness(exclusion_threshold=-1.0, minimum_measurements=5)
    with pytest.raises(OverflowError):
        LatencyAwareness(exclusion_threshold=3.0, minimum_measurements=-1)
    with pytest.raises(ValueError):
        LatencyAwareness(exclusion_threshold=3.0, minimum_measurements=5, retry_period_secs=-1)
    with pytest.raises(ValueError):
        LatencyAwareness(exclusion_threshold=3.0, minimum_measurements=5, update_rate_secs=-1)
    with pytest.raises(ValueError):
        LatencyAwareness(exclusion_threshold=3.0, minimum_measurements=5, scale_secs=-1)


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_default_policy_basic_queries() -> None:
    session = await SessionBuilder(
        ["127.0.0.2"], 9042, execution_profile=ExecutionProfile(policy=DefaultPolicy())
    ).connect()
    for _ in range(5):
        assert await session.execute("SELECT * FROM system.local") is not None


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_default_policy_with_all_options() -> None:
    la = LatencyAwareness(exclusion_threshold=3.0, minimum_measurements=5)
    policy = DefaultPolicy(
        preferred_datacenter="datacenter1",
        token_aware=True,
        permit_dc_failover=True,
        latency_awareness=la,
        enable_shuffling_replicas=False,
    )
    session = await SessionBuilder(["127.0.0.2"], 9042, execution_profile=ExecutionProfile(policy=policy)).connect()
    assert await session.execute("SELECT * FROM system.local") is not None


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_custom_policy_called_with_cluster_state() -> None:
    policy = TrackingPolicy()
    session = await SessionBuilder(["127.0.0.2"], 9042, execution_profile=ExecutionProfile(policy=policy)).connect()
    for _ in range(5):
        await session.execute("SELECT * FROM system.local")
    assert policy.call_count == 5
    assert len(policy.nodes_seen) == 15  # 5 * 3 nodes
    assert policy.nodes_seen[0]["host_id"] and policy.nodes_seen[0]["address"]


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_shard_aware_custom_policy() -> None:
    class ShardPolicy:
        def fallback(self, routing_info: RoutingInfo, cluster_state: ClusterState) -> Iterator[NodeShard]:
            return iter([NodeShard(n.host_id, i % 2) for i, n in enumerate(cluster_state.get_nodes_info())])

    session = await SessionBuilder(
        ["127.0.0.2"], 9042, execution_profile=ExecutionProfile(policy=ShardPolicy())
    ).connect()
    assert await session.execute("SELECT * FROM system.local") is not None


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_default_policy_preferred_dc_and_rack() -> None:
    """DefaultPolicy with preferred_datacenter_and_rack runs queries."""
    policy = DefaultPolicy(preferred_datacenter_and_rack=("datacenter1", "rack1"))
    session = await SessionBuilder(["127.0.0.2"], 9042, execution_profile=ExecutionProfile(policy=policy)).connect()
    assert await session.execute("SELECT * FROM system.local") is not None
