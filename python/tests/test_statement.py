from typing import Any, cast

import pytest
from scylla.errors import PrepareError, StatementConversionError
from scylla.session_builder import SessionBuilder
from scylla.statement import PreparedStatement, Statement


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_prepare_statement_with_str():
    session = await SessionBuilder().contact_points([("127.0.0.2", 9042)]).connect()
    prepared = await session.prepare("SELECT * FROM system.local")
    print(prepared)


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_prepare_statement_with_statement():
    session = await SessionBuilder().contact_points([("127.0.0.2", 9042)]).connect()
    statement = Statement("SELECT * FROM system.local")
    assert isinstance(statement, Statement)
    prepared = await session.prepare(statement)
    print(prepared)


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_prepare_and_execute():
    session = await SessionBuilder().contact_points([("127.0.0.2", 9042)]).connect()
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
    session = await SessionBuilder().contact_points([("127.0.0.2", 9042)]).connect()
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


def test_statement_with_page_size():
    query_str = "SELECT cluster_name FROM system.local;"
    statement = Statement(query_str)

    expected_page_size = 500
    statement = statement.with_page_size(expected_page_size)

    actual_page_size = statement.get_page_size()

    assert isinstance(actual_page_size, int)
    assert actual_page_size == expected_page_size


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_prepare_prepared_statement_raises_session_query_error():
    session = await SessionBuilder().contact_points([("127.0.0.2", 9042)]).connect()

    prepared = await session.prepare("SELECT * FROM system.local")

    with pytest.raises(PrepareError) as exc_info:
        await session.prepare(cast(Any, prepared))

    assert "cannot prepare a preparedstatement" in str(exc_info.value).lower()


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_prepare_invalid_query_raises_session_query_error():
    session = await SessionBuilder().contact_points([("127.0.0.2", 9042)]).connect()

    with pytest.raises(PrepareError) as exc_info:
        await session.prepare("THIS IS NOT CQL")

    assert "failed to prepare statement" in str(exc_info.value).lower()


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_prepare_invalid_statement_type_raises_statement_conversion_error():
    session = await SessionBuilder().contact_points([("127.0.0.2", 9042)]).connect()

    with pytest.raises(StatementConversionError) as exc_info:
        await session.prepare(123)  # type: ignore[arg-type]

    assert "invalid statement type" in str(exc_info.value).lower()
    assert "expected a str, statement, or preparedstatement" in str(exc_info.value).lower()


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_execute_invalid_statement_type_raises_statement_conversion_error():
    session = await SessionBuilder().contact_points([("127.0.0.2", 9042)]).connect()

    with pytest.raises(StatementConversionError) as exc_info:
        await session.execute(cast(Any, 123))

    assert "invalid statement type" in str(exc_info.value).lower()
