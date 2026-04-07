import pytest
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
async def test_prepared_statement_prepared_metadata():
    builder = SessionBuilder(["127.0.0.2"], 9042)
    session = await builder.connect()

    prepared = await session.prepare("SELECT cluster_name FROM system.local WHERE key = ?")

    metadata = prepared.prepared_metadata

    assert metadata is not None
    assert len(metadata.columns) == 1
    assert len(metadata.partition_key_indexes) == 1

    bind_col = metadata.columns[0]
    pk_index = metadata.partition_key_indexes[0]

    assert bind_col.name == "key"
    assert bind_col.table_name == "local"
    assert bind_col.keyspace_name == "system"
    assert isinstance(bind_col.cql_type, str)
    assert bind_col.cql_type != ""

    assert pk_index.index == 0
    assert pk_index.sequence_number == 0


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_prepared_statement_result_metadata():
    builder = SessionBuilder(["127.0.0.2"], 9042)
    session = await builder.connect()

    prepared = await session.prepare("SELECT cluster_name FROM system.local WHERE key = ?")

    metadata = prepared.result_metadata

    assert metadata is not None
    assert metadata.column_count == 1
    assert len(metadata.columns) == 1

    result_col = metadata.columns[0]

    assert result_col.name == "cluster_name"
    assert result_col.table_name == "local"
    assert result_col.keyspace_name == "system"
    assert isinstance(result_col.cql_type, str)
    assert result_col.cql_type != ""
