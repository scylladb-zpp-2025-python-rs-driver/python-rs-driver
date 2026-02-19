import uuid
from collections.abc import Iterator

import pytest
from scylla.cluster import ClusterState, NodeShard
from scylla.policies.load_balancing import DefaultPolicy, LoadBalancingPolicy, RoutingInfo


def test_node_shard() -> None:
    h = uuid.uuid4()
    ns = NodeShard(h, 3)
    assert ns.host_id == h and ns.shard == 3 and NodeShard(h, None).shard is None
    with pytest.raises(Exception):
        NodeShard("bad", None)  # type: ignore[arg-type]
    with pytest.raises(Exception):
        NodeShard(None, None)  # type: ignore[arg-type]


def test_protocol_conformance() -> None:
    class Good:
        def pick_targets(self, routing_info: RoutingInfo, cluster_state: ClusterState) -> Iterator[NodeShard]:
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
