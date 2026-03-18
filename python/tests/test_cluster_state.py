import ipaddress
import uuid
from types import MappingProxyType
from typing import AsyncGenerator

import pytest
import pytest_asyncio
from scylla.cluster import ClusterState, Node
from scylla.cluster.metadata import ColumnKind, Keyspace, StrategyKind
from scylla.routing import ReplicaLocator, Shard, Token
from scylla.session import Session
from scylla.session_builder import SessionBuilder

KEYSPACE = "cs_test_ks"
TABLE = "cs_test_table"


async def set_up() -> Session:
    builder = SessionBuilder(["127.0.0.2"], 9042)
    session = await builder.connect()
    await session.execute(f"""
        CREATE KEYSPACE IF NOT EXISTS {KEYSPACE}
        WITH replication = {{'class': 'NetworkTopologyStrategy', 'replication_factor': 1}};
    """)
    await session.execute(f"""
        CREATE TABLE IF NOT EXISTS {KEYSPACE}.{TABLE}
        (id int PRIMARY KEY, name text);
    """)
    return session


@pytest_asyncio.fixture(scope="module")
async def session() -> AsyncGenerator[Session, None]:
    s = await set_up()
    yield s
    await s.execute(f"DROP KEYSPACE IF EXISTS {KEYSPACE}")


@pytest_asyncio.fixture(scope="module")
async def cluster_state(session: Session) -> ClusterState:
    return session.cluster_state


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_cluster_state_type(cluster_state: ClusterState) -> None:
    assert isinstance(cluster_state, ClusterState)


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_get_keyspaces_returns(cluster_state: ClusterState) -> None:
    ks = cluster_state.keyspaces
    assert isinstance(ks, MappingProxyType)
    assert "system" in ks
    assert isinstance(ks["system"], Keyspace)
    assert KEYSPACE in ks
    assert isinstance(ks[KEYSPACE], Keyspace)


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_get_keyspace(cluster_state: ClusterState) -> None:
    assert isinstance(cluster_state.get_keyspace("system"), Keyspace)
    assert cluster_state.get_keyspace(KEYSPACE) is cluster_state.keyspaces[KEYSPACE]
    assert cluster_state.get_keyspace("no_such_keyspace_xyz") is None


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_keyspace_tables(cluster_state: ClusterState) -> None:
    ks = cluster_state.get_keyspace(KEYSPACE)
    assert ks is not None
    assert isinstance(ks.tables, MappingProxyType)
    assert TABLE in ks.tables


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_keyspace_views(cluster_state: ClusterState) -> None:
    ks = cluster_state.get_keyspace(KEYSPACE)
    assert ks is not None
    assert isinstance(ks.views, MappingProxyType)


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_keyspace_strategy(cluster_state: ClusterState) -> None:
    ks = cluster_state.get_keyspace("system")
    assert ks is not None
    assert ks.strategy.kind == StrategyKind.Local


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_table_partition_key(cluster_state: ClusterState) -> None:
    ks = cluster_state.get_keyspace(KEYSPACE)
    assert ks is not None
    pk = ks.tables[TABLE].partition_key
    assert isinstance(pk, MappingProxyType)
    assert "Native(Int)" in list(pk.values())[0].typ
    assert "id" in pk.keys()


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_table_columns_has_expected_names(cluster_state: ClusterState) -> None:
    ks = cluster_state.get_keyspace(KEYSPACE)
    assert ks is not None
    cols = ks.tables[TABLE].columns
    assert isinstance(cols, MappingProxyType)
    assert "id" in cols
    assert "name" in cols


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_column_kind_partition_key(cluster_state: ClusterState) -> None:
    ks = cluster_state.get_keyspace(KEYSPACE)
    assert ks is not None
    assert ks.tables[TABLE].columns["id"].kind == ColumnKind.PartitionKey


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_node_type(cluster_state: ClusterState) -> None:
    assert isinstance(cluster_state.nodes_info, MappingProxyType)


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_node_host_id_is_uuid(cluster_state: ClusterState) -> None:
    assert isinstance(list(cluster_state.nodes_info.items())[0][0], uuid.UUID)


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_node_host_address(cluster_state: ClusterState) -> None:
    assert isinstance(list(cluster_state.nodes_info.items())[0][1].address[0], ipaddress.IPv4Address)
    assert isinstance(list(cluster_state.nodes_info.items())[0][1].address[1], int)


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_compute_token_returns_token(cluster_state: ClusterState) -> None:
    token = cluster_state.compute_token(KEYSPACE, TABLE, {"id": 42})
    assert isinstance(token, Token)


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_replica_locator_type(cluster_state: ClusterState) -> None:
    assert isinstance(cluster_state.replica_locator, ReplicaLocator)


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_replica_locator_unique_nodes_in_global_ring(cluster_state: ClusterState) -> None:
    nodes = cluster_state.replica_locator.unique_token_owning_nodes_in_cluster()
    assert isinstance(nodes, list)
    assert len(nodes) > 0
    assert all(isinstance(n, Node) for n in nodes)


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_replica_locator_datacenter_names(cluster_state: ClusterState) -> None:
    dcs = cluster_state.replica_locator.datacenter_names
    assert isinstance(dcs, list)
    assert len(dcs) > 0


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_replica_set_iteration(cluster_state: ClusterState) -> None:
    ks = cluster_state.get_keyspace(KEYSPACE)
    assert ks is not None
    token = cluster_state.compute_token(KEYSPACE, TABLE, (1,))
    replicas = cluster_state.replica_locator.all_replicas_for_token(token, ks.strategy, KEYSPACE, TABLE)
    all_nodes = cluster_state.nodes_info
    assert isinstance(replicas, list)
    assert len(replicas) > 0
    for node, shard in replicas:
        info_node = all_nodes.get(node.host_id)
        assert info_node is node
        assert isinstance(node, Node)
        assert isinstance(shard, int)
        assert isinstance(shard, Shard)


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_get_token_endpoints(cluster_state: ClusterState) -> None:
    token = cluster_state.compute_token(KEYSPACE, TABLE, [42])
    replicas = cluster_state.get_token_endpoints(KEYSPACE, TABLE, token)
    assert isinstance(replicas, list)
    assert len(replicas) > 0
    for node, shard in replicas:
        assert isinstance(node, Node)
        assert isinstance(shard, int)
        assert isinstance(shard, Shard)


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_get_endpoints(cluster_state: ClusterState) -> None:
    replicas = cluster_state.get_endpoints(KEYSPACE, TABLE, [42])
    assert isinstance(replicas, list)
    assert len(replicas) > 0
    for node, shard in replicas:
        assert isinstance(node, Node)
        assert isinstance(shard, int)
        assert isinstance(shard, Shard)
