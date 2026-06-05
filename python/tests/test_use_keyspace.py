import pytest
from scylla.errors import ExecuteError, UseKeyspaceError
from scylla.session_builder import SessionBuilder


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_use_keyspace():
    session = await SessionBuilder().contact_points([("127.0.0.2", 9042)]).connect()
    await session.execute(
        "CREATE KEYSPACE IF NOT EXISTS test_keyspace WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}"
    )
    await session.execute("CREATE TABLE IF NOT EXISTS test_keyspace.test_table (id int PRIMARY KEY)")

    await session.use_keyspace("test_keyspace")
    await session.execute("INSERT INTO test_table (id) VALUES (1)")


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_use_non_existing_keyspace():
    session = await SessionBuilder().contact_points([("127.0.0.2", 9042)]).connect()
    with pytest.raises(UseKeyspaceError):
        await session.use_keyspace("non_existing_keyspace")


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_use_keyspace_case_sensitive():
    session = await SessionBuilder().contact_points([("127.0.0.2", 9042)]).connect()
    await session.execute(
        "CREATE KEYSPACE IF NOT EXISTS \"CaseSensitiveKeyspace\" WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}"
    )
    await session.execute('CREATE TABLE IF NOT EXISTS "CaseSensitiveKeyspace".test_table (id int PRIMARY KEY)')

    with pytest.raises(UseKeyspaceError):
        await session.use_keyspace("CaseSensitiveKeyspace")

    await session.use_keyspace("CaseSensitiveKeyspace", case_sensitive=True)
    await session.execute("INSERT INTO test_table (id) VALUES (1)")


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_change_use_keyspace():
    session = await SessionBuilder().contact_points([("127.0.0.2", 9042)]).connect()
    await session.execute(
        "CREATE KEYSPACE IF NOT EXISTS test_keyspace WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}"
    )
    await session.execute(
        "CREATE KEYSPACE IF NOT EXISTS other_keyspace WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}"
    )
    await session.execute("CREATE TABLE IF NOT EXISTS test_keyspace.test_table (id int PRIMARY KEY)")

    await session.use_keyspace("test_keyspace")
    await session.execute("INSERT INTO test_table (id) VALUES (1)")

    await session.use_keyspace("other_keyspace")

    with pytest.raises(ExecuteError):
        await session.execute("INSERT INTO test_table (id) VALUES (1)")
