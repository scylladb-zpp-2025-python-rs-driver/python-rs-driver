from typing import Iterator

import pytest
from scylla.cluster import ClusterState, NodeShard
from scylla.execution_profile import ExecutionProfile
from scylla.policies.load_balancing import RoutingInfo
from scylla.session_builder import SessionBuilder
from scylla.statement import PreparedStatement, Statement


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_prepare_statement_with_str():
    builder = SessionBuilder(["127.0.0.2"], 9042)
    session = await builder.connect()
    prepared = await session.prepare("SELECT * FROM system.local")
    print(prepared)


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_prepare_statement_with_statement():
    builder = SessionBuilder(["127.0.0.2"], 9042)
    session = await builder.connect()
    statement = Statement("SELECT * FROM system.local")
    assert isinstance(statement, Statement)
    prepared = await session.prepare(statement)
    print(prepared)


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_prepare_and_execute():
    builder = SessionBuilder(["127.0.0.2"], 9042)
    session = await builder.connect()
    query_str = "SELECT cluster_name FROM system.local"
    prepare_with_statement = await session.prepare(Statement(query_str))
    prepared_with_str = await session.prepare(query_str)
    assert isinstance(prepared_with_str, PreparedStatement)
    assert isinstance(prepare_with_statement, PreparedStatement)
    result_str = await session.execute(prepared_with_str)
    result_statement = await session.execute(prepare_with_statement)

    row_str = await result_str.first_row()
    row_statement = await result_statement.first_row()
    assert row_str is not None
    assert row_statement is not None
    assert row_str["cluster_name"] == row_statement["cluster_name"]


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_prepare_and_str():
    builder = SessionBuilder(["127.0.0.2"], 9042)
    session = await builder.connect()
    query_str = "SELECT cluster_name FROM system.local;"
    statement = Statement(query_str)
    prepared = await session.prepare(query_str)
    result_prepared = await session.execute(prepared)
    result_statement = await session.execute(statement)
    result_str = await session.execute(query_str)

    row_str = await result_str.first_row()
    row_prepared = await result_prepared.first_row()
    row_statement = await result_statement.first_row()

    assert row_str is not None
    assert row_prepared is not None
    assert row_statement is not None

    cluster_name_str = row_str["cluster_name"]
    assert row_prepared["cluster_name"] == cluster_name_str
    assert cluster_name_str == row_statement["cluster_name"]


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_statement_with_page_size():
    query_str = "SELECT cluster_name FROM system.local;"
    statement = Statement(query_str)

    expected_page_size = 500
    statement = statement.with_page_size(expected_page_size)

    actual_page_size = statement.get_page_size()

    assert isinstance(actual_page_size, int)
    assert actual_page_size == expected_page_size


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_prepare_statement_propagates_lbp():
    builder = SessionBuilder(["127.0.0.2"], 9042)
    session = await builder.connect()

    class DummyPolicy:
        counter = 0

        def pick_targets(self, routing_info: RoutingInfo, cluster_state: ClusterState) -> Iterator[NodeShard]:
            self.counter += 1
            return iter([n.node_shard for n in cluster_state.get_nodes_info()])

    class DifferentDummyPolicy:
        counter = 0

        def pick_targets(self, routing_info: RoutingInfo, cluster_state: ClusterState) -> Iterator[NodeShard]:
            self.counter += 1
            return iter([n.node_shard for n in cluster_state.get_nodes_info()])

    policy = DummyPolicy()
    profile = ExecutionProfile(policy=DifferentDummyPolicy())
    stmt = Statement("SELECT * FROM system.local").with_load_balancing_policy(policy).with_execution_profile(profile)
    prepared = await session.prepare(stmt)
    _ = await session.execute(prepared)
    assert policy.counter == 1
    assert profile.get_load_balancing_policy().counter == 0  # pyright: ignore[reportUnknownMemberType, reportOptionalMemberAccess, reportAttributeAccessIssue]
    assert prepared.get_load_balancing_policy() is policy
    assert profile.get_load_balancing_policy() is prepared.get_execution_profile().get_load_balancing_policy()  # pyright: ignore[reportOptionalMemberAccess]
    assert profile is prepared.get_execution_profile()
