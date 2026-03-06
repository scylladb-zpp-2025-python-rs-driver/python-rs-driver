from __future__ import annotations

import asyncio
from typing import Iterator

from scylla.cluster import ClusterState, NodeShard
from scylla.execution_profile import ExecutionProfile
from scylla.policies.load_balancing import RoutingInfo
from scylla.session_builder import SessionBuilder
from scylla.statement import Statement


class CustomRoundRobinLoadBalancingPolicy:
    def __init__(self) -> None:
        self.count = 0
        self.index: int = 0

    # This showcases how `pick_targets()` can function as an arbitrary callback on each `session.execute()`
    def pick_targets(self, routing_info: RoutingInfo, cluster_state: ClusterState) -> Iterator[NodeShard]:
        token = routing_info.token
        assert token is not None
        locator = cluster_state.replica_locator()
        ks = cluster_state.get_keyspace("example_ks")
        assert ks is not None
        self.count += 1
        print(f"call count: {self.count}")
        print(ks)
        print(ks.strategy)
        strat = ks.strategy
        set = locator.replicas_for_token(
            token=token,
            strategy=strat,
            datacenter="datacenter1",
            keyspace="example_ks",
            table="t",
        )
        print(f"length of replica set returned: {len(set)}")
        return iter([n.node_shard for n in cluster_state.get_nodes_info()])


async def main():
    builder = SessionBuilder(["127.0.0.2"], 9042)
    session = await builder.connect()
    await session.execute("DROP TABLE IF EXISTS example_ks.t")
    await session.execute("DROP KEYSPACE IF EXISTS example_ks")
    await session.execute(
        "CREATE KEYSPACE IF NOT EXISTS example_ks WITH REPLICATION = {'class': 'NetworkTopologyStrategy', 'replication_factor': 3}"
    )
    await session.execute("CREATE TABLE IF NOT EXISTS example_ks.t (a int PRIMARY KEY, b text, c int)")

    await session.execute(Statement("INSERT INTO example_ks.t (a, b, c) VALUES (1, 'one', 1)"))

    policy = CustomRoundRobinLoadBalancingPolicy()

    prepared_statement = (
        await session.prepare(
            "INSERT INTO example_ks.t (a, b, c) VALUES (?, ?, ?)",
        )
    ).with_execution_profile(ExecutionProfile(policy=policy))

    cor = [session.execute(prepared_statement, (i, f"{i}", -1 * i)) for i in range(10)]
    await asyncio.gather(*cor)

    res = await session.execute(Statement("SELECT * FROM example_ks.t"))
    for r in res.iter_rows():
        print(r)


asyncio.run(main())
