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
    assert list(result_prepared.iter_current_page()) == list(result_str.iter_current_page())


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_prepared_with_and_get_page_size():
    builder = SessionBuilder(["127.0.0.2"], 9042)
    session = await builder.connect()

    prepared = await session.prepare("SELECT * FROM system.local")

    expected_page_size = 500
    prepared = prepared.with_page_size(expected_page_size)

    actual_page_size = prepared.get_page_size()

    assert isinstance(actual_page_size, int)
    assert actual_page_size == expected_page_size
