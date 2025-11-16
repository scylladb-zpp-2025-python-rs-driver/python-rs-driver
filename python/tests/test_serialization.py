"""
Test extracting RowSerializationContext from prepared statements and inserting data
with proper type extraction and validation using ValueList
"""

import pytest
from scylla._rust.session import PyRowSerializationContext
from scylla.serialize import column_type
from scylla.serialize.serialize import (
    BigInt,
    Boolean,
    Double,
    Int,
    List,
    Text,
    UserDefinedType,
    ValueList,
)
from scylla.session import Session
from scylla.session_builder import SessionBuilder


@pytest.mark.asyncio
async def test_context_native_types():
    """Extract RowSerializationContext with native types and insert data using ValueList"""
    builder = SessionBuilder(["127.0.0.2"], 9042)
    session: Session = await builder.connect()

    _ = await session.execute(
        "CREATE KEYSPACE IF NOT EXISTS test_ks WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 1}"
    )
    _ = await session.execute("USE test_ks")
    _ = await session.execute("""
        CREATE TABLE IF NOT EXISTS ctx_native_types (
            id int PRIMARY KEY,
            bigint_val bigint,
            double_val double,
            boolean_val boolean,
            text_val text
        )
    """)

    # Prepare and extract context
    prepared = await session.prepare(
        "INSERT INTO ctx_native_types (id, bigint_val, double_val, boolean_val, text_val) VALUES (?, ?, ?, ?, ?)"
    )

    py_ctx = PyRowSerializationContext.from_prepared(prepared)
    row_ctx = py_ctx.get_context()

    # Display extracted context
    print("\n=== RowSerializationContext for ctx_native_types ===")
    print(
        f"Table: {row_ctx.columns[0].table_spec.ks_name}.{row_ctx.columns[0].table_spec.table_name}"
    )
    print(f"Columns ({len(row_ctx.columns)}):")
    for i, col_spec in enumerate(row_ctx.columns):
        print(f"  [{i}] {col_spec.name}: {col_spec.typ}")

    # Verify context structure
    assert isinstance(row_ctx, column_type.RowSerializationContext)
    assert len(row_ctx.columns) == 5

    # Verify each column
    assert row_ctx.columns[0].name == "id"
    assert row_ctx.columns[0].table_spec.ks_name == "test_ks"
    assert row_ctx.columns[0].table_spec.table_name == "ctx_native_types"
    assert isinstance(row_ctx.columns[0].typ, column_type.Native)
    assert row_ctx.columns[0].typ.type == column_type.NativeType.INT

    assert row_ctx.columns[1].name == "bigint_val"
    assert row_ctx.columns[1].typ.type == column_type.NativeType.BIGINT

    assert row_ctx.columns[2].name == "double_val"
    assert row_ctx.columns[2].typ.type == column_type.NativeType.DOUBLE

    assert row_ctx.columns[3].name == "boolean_val"
    assert row_ctx.columns[3].typ.type == column_type.NativeType.BOOLEAN

    assert row_ctx.columns[4].name == "text_val"
    assert row_ctx.columns[4].typ.type == column_type.NativeType.TEXT

    # Insert test data using ValueList
    test_data = [
        (1, 1000000000000, 3.14159, True, "First row"),
        (2, -9223372036854775808, -273.15, False, "Negative values"),
        (3, 0, 0.0, True, "Zero values"),
        (4, 9223372036854775807, 1.7976931348623157e308, False, "Max values"),
    ]

    for test_id, bigint_v, double_v, bool_v, text_v in test_data:
        # Create ValueList with all values
        value_list = ValueList(
            [
                Int(test_id),
                BigInt(bigint_v),
                Double(double_v),
                Boolean(bool_v),
                Text(text_v),
            ]
        )

        # Execute with ValueList - it will use the context automatically
        result = await session.execute_with_column_spec(prepared, value_list)
        print(f"✓ Inserted row {test_id}")

    print("✓ All native type insertions successful\n")


@pytest.mark.asyncio
async def test_context_list_types():
    """Extract RowSerializationContext with list types and insert data using ValueList"""
    builder = SessionBuilder(["127.0.0.2"], 9042)
    session = await builder.connect()

    _ = await session.execute("USE test_ks")
    _ = await session.execute("""
        CREATE TABLE IF NOT EXISTS ctx_list_types (
            id int PRIMARY KEY,
            int_list list<int>,
            text_list list<text>,
            bigint_list list<bigint>
        )
    """)

    # Prepare and extract context
    prepared = await session.prepare(
        "INSERT INTO ctx_list_types (id, int_list, text_list, bigint_list) VALUES (?, ?, ?, ?)"
    )

    py_ctx = PyRowSerializationContext.from_prepared(prepared)
    row_ctx = py_ctx.get_context()

    # Display extracted context
    print(f"=== RowSerializationContext for ctx_list_types ===")
    print(
        f"Table: {row_ctx.columns[0].table_spec.ks_name}.{row_ctx.columns[0].table_spec.table_name}"
    )
    print(f"Columns ({len(row_ctx.columns)}):")
    for i, col_spec in enumerate(row_ctx.columns):
        if isinstance(col_spec.typ, column_type.List):
            print(
                f"  [{i}] {col_spec.name}: List<{col_spec.typ.element_type}> (frozen={col_spec.typ.frozen})"
            )
        else:
            print(f"  [{i}] {col_spec.name}: {col_spec.typ}")

    # Verify context structure
    assert isinstance(row_ctx, column_type.RowSerializationContext)
    assert len(row_ctx.columns) == 4

    # Verify list columns
    assert row_ctx.columns[1].name == "int_list"
    assert isinstance(row_ctx.columns[1].typ, column_type.List)
    assert row_ctx.columns[1].typ.frozen == False
    assert isinstance(row_ctx.columns[1].typ.element_type, column_type.Native)
    assert row_ctx.columns[1].typ.element_type.type == column_type.NativeType.INT

    assert row_ctx.columns[2].name == "text_list"
    assert isinstance(row_ctx.columns[2].typ, column_type.List)
    assert row_ctx.columns[2].typ.element_type.type == column_type.NativeType.TEXT

    assert row_ctx.columns[3].name == "bigint_list"
    assert isinstance(row_ctx.columns[3].typ, column_type.List)
    assert row_ctx.columns[3].typ.element_type.type == column_type.NativeType.BIGINT

    # Insert test data using ValueList
    test_data = [
        (1, [], [], []),  # Empty lists
        (2, [1, 2, 3], ["one", "two", "three"], [100, 200, 300]),
        (3, [42], ["single"], [999999999999]),
        (4, [-1, 0, 1], ["", "empty", "test"], [-1000, 0, 1000]),
    ]

    for test_id, int_list, text_list, bigint_list in test_data:
        value_list = ValueList(
            [
                Int(test_id),
                List([Int(v) for v in int_list]),
                List([Text(v) for v in text_list]),
                List([BigInt(v) for v in bigint_list]),
            ]
        )

        result = await session.execute_with_column_spec(prepared, value_list)
        print(
            f"✓ Inserted row {test_id}: int_list={int_list}, text_list={text_list}, bigint_list={bigint_list}"
        )

    print("✓ All list type insertions successful\n")


@pytest.mark.asyncio
async def test_context_udt_types():
    """Extract RowSerializationContext with UDT types and insert data using ValueList"""
    builder = SessionBuilder(["127.0.0.2"], 9042)
    session = await builder.connect()

    _ = await session.execute("USE test_ks")

    # Drop and recreate to ensure clean state
    _ = await session.execute("DROP TABLE IF EXISTS ctx_udt_types")
    _ = await session.execute("DROP TYPE IF EXISTS address")

    _ = await session.execute("""
        CREATE TYPE address (
            street text,
            city text,
            zip_code int
        )
    """)

    _ = await session.execute("""
        CREATE TABLE ctx_udt_types (
            id int PRIMARY KEY,
            user_address address
        )
    """)

    # Prepare and extract context
    prepared = await session.prepare(
        "INSERT INTO ctx_udt_types (id, user_address) VALUES (?, ?)"
    )

    py_ctx = PyRowSerializationContext.from_prepared(prepared)
    row_ctx = py_ctx.get_context()

    # Display extracted context
    print(f"=== RowSerializationContext for ctx_udt_types ===")
    print(
        f"Table: {row_ctx.columns[0].table_spec.ks_name}.{row_ctx.columns[0].table_spec.table_name}"
    )
    print(f"Columns ({len(row_ctx.columns)}):")
    for i, col_spec in enumerate(row_ctx.columns):
        if isinstance(col_spec.typ, column_type.UserDefinedType):
            print(
                f"  [{i}] {col_spec.name}: UDT '{col_spec.typ.definition.name}' (frozen={col_spec.typ.frozen})"
            )
            print(f"       Keyspace: {col_spec.typ.definition.keyspace}")
            print(f"       Fields:")
            for field_name, field_type in col_spec.typ.definition.field_types:
                print(f"         - {field_name}: {field_type}")
        else:
            print(f"  [{i}] {col_spec.name}: {col_spec.typ}")

    # Verify context structure
    assert isinstance(row_ctx, column_type.RowSerializationContext)
    assert len(row_ctx.columns) == 2

    # Verify UDT column
    assert row_ctx.columns[1].name == "user_address"
    assert isinstance(row_ctx.columns[1].typ, column_type.UserDefinedType)
    assert row_ctx.columns[1].typ.frozen == False
    assert row_ctx.columns[1].typ.definition.name == "address"
    assert row_ctx.columns[1].typ.definition.keyspace == "test_ks"
    assert len(row_ctx.columns[1].typ.definition.field_types) == 3

    # Verify UDT field structure
    field_names = [name for name, _ in row_ctx.columns[1].typ.definition.field_types]
    assert "street" in field_names
    assert "city" in field_names
    assert "zip_code" in field_names

    # Insert test data using ValueList
    test_addresses = [
        (1, {"street": "123 Main St", "city": "Springfield", "zip_code": 12345}),
        (2, {"street": "456 Oak Ave", "city": "Shelbyville", "zip_code": 67890}),
        (3, {"street": "789 Elm St", "city": "Capital City", "zip_code": 11111}),
        (4, {"street": "", "city": "Empty Street", "zip_code": 0}),
    ]

    for test_id, addr_data in test_addresses:
        address_dict = {
            "street": Text(addr_data["street"]),
            "city": Text(addr_data["city"]),
            "zip_code": Int(addr_data["zip_code"]),
        }

        value_list = ValueList([Int(test_id), UserDefinedType(address_dict)])

        result = await session.execute_with_column_spec(prepared, value_list)
        print(f"✓ Inserted row {test_id}: {addr_data}")

    print("✓ All UDT insertions successful\n")


@pytest.mark.asyncio
async def test_context_mixed_types():
    """Extract RowSerializationContext with mixed types and insert data using ValueList"""
    builder = SessionBuilder(["127.0.0.2"], 9042)
    session = await builder.connect()

    _ = await session.execute("USE test_ks")
    _ = await session.execute("""
        CREATE TABLE IF NOT EXISTS ctx_mixed_types (
            user_id int PRIMARY KEY,
            username text,
            age int,
            scores list<int>,
            balance bigint,
            rating double,
            is_active boolean
        )
    """)

    # Prepare and extract context
    prepared = await session.prepare(
        "INSERT INTO ctx_mixed_types (user_id, username, age, scores, balance, rating, is_active) VALUES (?, ?, ?, ?, ?, ?, ?)"
    )

    py_ctx = PyRowSerializationContext.from_prepared(prepared)
    row_ctx = py_ctx.get_context()

    # Display extracted context
    print(f"=== RowSerializationContext for ctx_mixed_types ===")
    print(
        f"Table: {row_ctx.columns[0].table_spec.ks_name}.{row_ctx.columns[0].table_spec.table_name}"
    )
    print(f"Columns ({len(row_ctx.columns)}):")
    for i, col_spec in enumerate(row_ctx.columns):
        if isinstance(col_spec.typ, column_type.List):
            print(f"  [{i}] {col_spec.name}: List<{col_spec.typ.element_type}>")
        else:
            print(f"  [{i}] {col_spec.name}: {col_spec.typ}")

    # Verify context structure
    assert isinstance(row_ctx, column_type.RowSerializationContext)
    assert len(row_ctx.columns) == 7

    # Verify column names and types
    assert row_ctx.columns[0].name == "user_id"
    assert row_ctx.columns[0].typ.type == column_type.NativeType.INT

    assert row_ctx.columns[1].name == "username"
    assert row_ctx.columns[1].typ.type == column_type.NativeType.TEXT

    assert row_ctx.columns[2].name == "age"
    assert row_ctx.columns[2].typ.type == column_type.NativeType.INT

    assert row_ctx.columns[3].name == "scores"
    assert isinstance(row_ctx.columns[3].typ, column_type.List)
    assert row_ctx.columns[3].typ.element_type.type == column_type.NativeType.INT

    assert row_ctx.columns[4].name == "balance"
    assert row_ctx.columns[4].typ.type == column_type.NativeType.BIGINT

    assert row_ctx.columns[5].name == "rating"
    assert row_ctx.columns[5].typ.type == column_type.NativeType.DOUBLE

    assert row_ctx.columns[6].name == "is_active"
    assert row_ctx.columns[6].typ.type == column_type.NativeType.BOOLEAN

    # Insert test data using ValueList
    test_users = [
        (1, "alice", 25, [100, 95, 88], 1000000, 4.5, True),
        (2, "bob", 30, [75, 80, 92], 2500000, 3.8, True),
        (3, "charlie", 22, [], 0, 0.0, False),
        (4, "diana", 28, [99, 100, 100, 98], 5000000, 4.9, True),
    ]

    for user_id, username, age, scores, balance, rating, is_active in test_users:
        value_list = ValueList(
            [
                Int(user_id),
                Text(username),
                Int(age),
                List([Int(s) for s in scores]),
                BigInt(balance),
                Double(rating),
                Boolean(is_active),
            ]
        )

        result = await session.execute_with_column_spec(prepared, value_list)
        print(
            f"✓ Inserted user {user_id}: {username}, age={age}, scores={scores}, balance={balance}, rating={rating}, active={is_active}"
        )

    print("✓ All mixed type insertions successful\n")


@pytest.mark.asyncio
async def test_valuelist_empty():
    """Test ValueList is_empty() method"""
    empty_list = ValueList([])
    assert empty_list.is_empty() == True

    non_empty_list = ValueList([Int(1), Text("test")])
    assert non_empty_list.is_empty() == False

    print("✓ ValueList is_empty() tests passed")


@pytest.mark.asyncio
async def test_context_verification():
    """Verify RowSerializationContext provides complete metadata and ValueList integration"""
    builder = SessionBuilder(["127.0.0.2"], 9042)
    session = await builder.connect()

    _ = await session.execute("USE test_ks")
    _ = await session.execute("""
        CREATE TABLE IF NOT EXISTS ctx_verification (
            pk int PRIMARY KEY,
            col1 text,
            col2 bigint
        )
    """)

    prepared = await session.prepare(
        "INSERT INTO ctx_verification (pk, col1, col2) VALUES (?, ?, ?)"
    )

    py_ctx = PyRowSerializationContext.from_prepared(prepared)
    row_ctx = py_ctx.get_context()

    print(f"=== Verification Test ===")
    print(f"Context type: {type(row_ctx)}")
    print(f"Context columns type: {type(row_ctx.columns)}")
    print(f"Number of columns: {len(row_ctx.columns)}")

    # Verify all metadata is accessible
    for i, col_spec in enumerate(row_ctx.columns):
        print(f"\nColumn {i} details:")
        print(f"  Type: {type(col_spec)}")
        print(f"  Name: {col_spec.name}")
        print(f"  Table keyspace: {col_spec.table_spec.ks_name}")
        print(f"  Table name: {col_spec.table_spec.table_name}")
        print(f"  Column type: {col_spec.typ}")
        print(f"  Column type class: {type(col_spec.typ)}")

        # Verify all required attributes exist
        assert hasattr(col_spec, "name")
        assert hasattr(col_spec, "table_spec")
        assert hasattr(col_spec, "typ")
        assert hasattr(col_spec.table_spec, "ks_name")
        assert hasattr(col_spec.table_spec, "table_name")

    # Test insertion with ValueList
    value_list = ValueList(
        [
            Int(1),
            Text("test_value"),
            BigInt(9999999999),
        ]
    )

    result = await session.execute_with_column_spec(prepared, value_list)
    print("\n✓ Inserted test row using ValueList")
    print("✓ All metadata verification passed")
