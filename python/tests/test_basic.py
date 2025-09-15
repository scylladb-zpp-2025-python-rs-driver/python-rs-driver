from scylla.session_builder import SessionBuilder;
import pytest

@pytest.mark.asyncio
async def test_cluster_connect():
    builder = SessionBuilder(['172.42.0.2'], 9042)
    session = await builder.connect()

@pytest.mark.asyncio
async def test_simple_query():
    builder = SessionBuilder(['172.42.0.2'], 9042)
    session = await builder.connect()
    result = await session.execute("SELECT * FROM system.local")
    print(result)
