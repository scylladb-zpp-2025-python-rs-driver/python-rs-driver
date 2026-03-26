import pytest
from scylla.session_builder import SessionBuilder


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_cluster_connect():
    _ = await SessionBuilder().contact_points([("127.0.0.2", 9042)]).connect()


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_simple_query():
    session = await SessionBuilder().contact_points([("127.0.0.2", 9042)]).connect()
    result = await session.execute("SELECT * FROM system.local")
    print(result)
