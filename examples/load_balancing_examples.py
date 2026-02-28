from __future__ import annotations

import asyncio
from typing import Iterator

from scylla.cluster import ClusterState, NodeShard
from scylla.policies.load_balancing import RoutingInfo
from scylla.session_builder import SessionBuilder
from scylla.statement import Statement


class CustomRoundRobinLoadBalancingPolicy:
    def __init__(self) -> None:
        self.index: int = 0

    # NOTE: this load balancing policy doesn't aim to do anything meaningful.
    # This is just to show work what we are working on.
    def fallback(self, routing_info: RoutingInfo, cluster_state: ClusterState) -> Iterator[NodeShard]:
        ks = cluster_state.keyspaces_iter()
        # This function can serve as a callback on each session.execute()
        # Let's print keyspaces on each call
        print(list(ks.keys()))

        # Normal Round Robin
        nodes = cluster_state.get_nodes_info()
        self.index = (self.index + 1) % len(nodes)

        # Let's calculate a shard for our table
        token = cluster_state.compute_token(keyspace="example_ks", table="t", partition_key=1)
        sharder = cluster_state.get_nodes_info()[self.index].sharder()
        assert sharder is not None
        shard = sharder.shard_of(token)

        return iter([NodeShard(nodes[self.index].host_id, shard=shard)])


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
    ).with_load_balancing_policy(policy)

    await session.execute(prepared_statement, (1, "{1}", -1))
    await session.execute(prepared_statement, (2, "{2}", -2))
    await session.execute(prepared_statement, (3, "{3}", -3))

    res = await session.execute(Statement("SELECT * FROM example_ks.t"))
    for r in res.iter_rows():
        print(r)


asyncio.run(main())
