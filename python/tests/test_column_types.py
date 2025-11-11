import pytest
from scylla.session import Session
from scylla.writers import PyRowSerializationContext
from scylla.column_type import *
from scylla.session_builder import SessionBuilder

from scylla.serializer import *


@pytest.mark.asyncio
async def test_columns_native_types():
    builder = SessionBuilder(["127.0.0.2"], 9042)
    session = await builder.connect()

    _ = await session.execute(
        "CREATE KEYSPACE IF NOT EXISTS test_ks WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 1}"
    )
    _ = await session.execute("USE test_ks")
    _ = await session.execute("""
                              CREATE TABLE IF NOT EXISTS basic_types (
                                                                         id int PRIMARY KEY,
                                                                         name text,
                                                                         score double,
                              )
                              """)

    prepared = await session.prepare(
        "INSERT INTO test_ks.basic_types (id, name, score) VALUES (?, ?, ?)"
    )

    ctx = PyRowSerializationContext.from_prepared(prepared)

    columns = ctx.get_columns()

    expected_types = ["Int", "Text", "Double"]
    expected_serializers = [IntSerializer, TextSerializer, DoubleSerializer]

    assert len(columns) == len(expected_types), "Column count mismatch"

    for i, (col, expected_type, expected_serializer) in enumerate(
            zip(columns, expected_types, expected_serializers)
    ):
        t = type(col).__name__
        assert t == expected_type, f"Column {i}: got {t}, expected {expected_type}"

        # --- Create serializer directly from PyO3 column type ---
        serializer = create_serializer_from_col_type(col)
        assert isinstance(serializer, expected_serializer), (
            f"Column {i}: expected {expected_serializer.__name__}, got {type(serializer).__name__}"
    )

@pytest.mark.asyncio
async def test_list_of_int_column():
    builder = SessionBuilder(["127.0.0.2"], 9042)
    session = await builder.connect()

    # Create keyspace and table
    await session.execute("""
        CREATE KEYSPACE IF NOT EXISTS test_ks
        WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1}
    """)
    await session.execute("USE test_ks")

    await session.execute("""
                          CREATE TABLE IF NOT EXISTS list_test (
                                                                   id int PRIMARY KEY,
                                                                   nums list<int>
                          )
                          """)

    # Prepare statement
    prepared = await session.prepare(
        "INSERT INTO test_ks.list_test (id, nums) VALUES (?, ?)"
    )

    # Extract types using your Rust wrapper
    ctx = PyRowSerializationContext.from_prepared(prepared)
    columns = ctx.get_columns()

    # Expected: [Int(), List(Int())]
    assert len(columns) == 2

    id_col, nums_col = columns

    assert isinstance(id_col, Int), f"Expected Int, got {type(id_col).__name__}"
    assert isinstance(id_col, PyNativeType)

    # Column 1: nums -> List(Int)
    assert isinstance(nums_col, List), f"Expected List, got {type(nums_col).__name__}"
    assert isinstance(nums_col, PyCollectionType)
    assert isinstance(nums_col.column_type, Int), (
        f"Expected List(Int), got List({type(nums_col.column_type).__name__})"
    )

