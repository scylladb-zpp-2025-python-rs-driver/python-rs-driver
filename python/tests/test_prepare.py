import pytest
from scylla.session_builder import SessionBuilder
from scylla.statement import PreparedStatement


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_prepare_statement():
    builder = SessionBuilder(["127.0.0.2"], 9042)
    session = await builder.connect()
    prepared = await session.prepare("SELECT * FROM system.local")
    print(prepared)


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_prepare_and_execute():
    builder = SessionBuilder(["127.0.0.2"], 9042)
    session = await builder.connect()
    prepared = await session.prepare("SELECT * FROM system.local")
    assert isinstance(prepared, PreparedStatement)
    result = await session.execute(prepared)
    print(result)


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_prepare_and_str():
    builder = SessionBuilder(["127.0.0.2"], 9042)
    session = await builder.connect()
    query_str = "SELECT cluster_name FROM system.local"
    prepared = await session.prepare(query_str)
    result_prepared = await session.execute(prepared)
    result_str = await session.execute(query_str)
    assert result_prepared.__str__() == result_str.__str__()
