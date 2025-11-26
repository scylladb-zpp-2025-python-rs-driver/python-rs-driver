import pytest
from scylla.session_builder import SessionBuilder


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_cluster_connect():
    builder = SessionBuilder(["127.0.0.2"], 9042)
    _ = await builder.connect()


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_simple_query():
    builder = SessionBuilder(["127.0.0.2"], 9042)
    session = await builder.connect()
    result = await session.execute("SELECT * FROM system.local")
    print(result)
