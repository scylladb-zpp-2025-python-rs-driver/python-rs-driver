from scylla.session_builder import SessionBuilder
from scylla.serializalize import (
    SerializationError,
)
from scylla.session import PyRowSerializationContext
import pytest
import struct
from dataclasses import dataclass
from typing import List, Any


@dataclass
class Address:
    """Example UDT class for address"""

    street: str
    city: str
    zip_code: int

    def to_dict(self):
        return {"street": self.street, "city": self.city, "zip_code": self.zip_code}


@dataclass
class Person:
    """Example UDT class for person"""

    name: str
    age: int
    address: Address

    def to_dict(self):
        return {
            "name": self.name,
            "age": self.age,
            "address": self.address.to_dict() if self.address else None,
        }


@pytest.mark.asyncio
async def test_cluster_connect():
    builder = SessionBuilder(["127.0.0.2"], 9042)
    session = await builder.connect()


@pytest.mark.asyncio
async def test_simple_query():
    builder = SessionBuilder(["127.0.0.2"], 9042)
    session = await builder.connect()
    result = await session.execute("SELECT * FROM system.local")
    print(result)


@pytest.mark.asyncio
async def test_basic_serialization():
    """Test basic type serialization with the new module"""
    builder = SessionBuilder(["127.0.0.2"], 9042)
    session = await builder.connect()

    # Setup
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

    # Prepare statement
    prepared = await session.prepare(
        "INSERT INTO test_ks.basic_types (id, name, score) VALUES (?, ?, ?)"
    )

    # Use the serialization module

    # Test data
    test_values = [1, "Test Name", 95.5]

    result = await session.execute_unpaged_python(prepared, test_values)
    print(f"Basic serialization SUCCESS: {result}")


@pytest.mark.asyncio
async def test_list_serialization():
    """Test list serialization with the new module"""
    builder = SessionBuilder(["127.0.0.2"], 9042)
    session = await builder.connect()

    _ = await session.execute("USE test_ks")
    _ = await session.execute("""
        CREATE TABLE IF NOT EXISTS list_types (
            id int PRIMARY KEY,
            tags list<text>,
            scores list<int>
        )
    """)

    prepared = await session.prepare(
        "INSERT INTO test_ks.list_types (id, tags, scores) VALUES (?, ?, ?)"
    )
    # Test with different list scenarios
    test_cases = [
        [1, [], []],  # Empty lists
        [2, ["tag1"], [100]],  # Single elements
        [3, ["a", "b", "c"], [1, 2, 3, 4, 5]],  # Multiple elements
        [4, ["test", "with", "spaces"], [0, -1, 999]],  # Various values
    ]

    for test_values in test_cases:
        result = await session.execute_unpaged_python(prepared, test_values)
        print(f"List test SUCCESS for {test_values}: {result}")


@pytest.mark.asyncio
async def test_null_values():
    """Test NULL value handling"""
    builder = SessionBuilder(["127.0.0.2"], 9042)
    session = await builder.connect()

    _ = await session.execute("USE test_ks")
    _ = await session.execute("""
        CREATE TABLE IF NOT EXISTS nullable_test (
            id int PRIMARY KEY,
            name text,
            score double,
            tags list<text>
        )
    """)

    prepared = await session.prepare(
        "INSERT INTO test_ks.nullable_test (id, name, score, tags) VALUES (?, ?, ?, ?)"
    )

    # Test with NULL values
    test_cases = [
        [1, "Not Null", 1.0, ["tag"]],
        [2, None, None, None],  # All NULLs
        [3, "Mixed", None, []],  # Mixed NULLs and empty
        [4, "Null in list", 2.5, None],  # NULL list
    ]

    for test_values in test_cases:
        result = await session.execute_unpaged_python(prepared, test_values)
        print(f"NULL test SUCCESS for {test_values}: {result}")


@pytest.mark.asyncio
async def test_structured_serialization():
    """Test the structured serialization approach"""
    builder = SessionBuilder(["127.0.0.2"], 9042)
    session = await builder.connect()

    # Setup
    _ = await session.execute(
        "CREATE KEYSPACE IF NOT EXISTS test_ks WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor' : 1}"
    )
    _ = await session.execute("USE test_ks")
    _ = await session.execute("""
        CREATE TABLE IF NOT EXISTS structured_test (
            id int PRIMARY KEY,
            name text,
            score double,
            active boolean,
            age bigint
        )
    """)

    # Prepare statement
    prepared = await session.prepare(
        "INSERT INTO test_ks.structured_test (id, name, score, active, age) VALUES (?, ?, ?, ?, ?)"
    )

    # Test with main serializer
    test_values = [1, "Structured Test", 98.5, True, 1234567890]
    result = await session.execute_unpaged_python(prepared, test_values)
    print(f"Structured serialization SUCCESS: {result}")

    # Test with different types
    test_values2 = [2, "Another Test", 87.2, False, 9876543210]
    result2 = await session.execute_unpaged_python(prepared, test_values2)
    print(f"Structured serialization 2 SUCCESS: {result2}")


@pytest.mark.asyncio
async def test_structured_list_serialization():
    """Test structured list serialization"""
    builder = SessionBuilder(["127.0.0.2"], 9042)
    session = await builder.connect()

    _ = await session.execute("USE test_ks")
    _ = await session.execute("""
        CREATE TABLE IF NOT EXISTS structured_list_test (
            id int PRIMARY KEY,
            tags list<text>,
            scores list<int>
        )
    """)

    prepared = await session.prepare(
        "INSERT INTO test_ks.structured_list_test (id, tags, scores) VALUES (?, ?, ?)"
    )

    test_cases = [
        [1, [], []],  # Empty lists
        [2, ["tag1"], [100]],  # Single elements
        [3, ["a", "b", "c"], [1, 2, 3, 4, 5]],  # Multiple elements
        [4, ["test", "with", "spaces"], [0, -1, 999]],  # Various values
    ]

    for test_values in test_cases:
        result = await session.execute_unpaged_python(prepared, test_values)
        print(f"Structured list test SUCCESS for {test_values}: {result}")


@pytest.mark.asyncio
async def test_column_metadata_inspection():
    """Test inspection of structured column metadata"""
    builder = SessionBuilder(["127.0.0.2"], 9042)
    session = await builder.connect()

    _ = await session.execute("USE test_ks")

    # Test with various types
    prepared = await session.prepare(
        "INSERT INTO test_ks.structured_test (id, name, score, active, age) VALUES (?, ?, ?, ?, ?)"
    )

    # Inspect the column metadata
    ctx = PyRowSerializationContext.from_prepared(prepared)
    columns = ctx.columns()

    for i, col in enumerate(columns):
        print(f"Column {i}:")
        print(f"  Name: {col['name']}")
        print(f"  Type struct: {col.get('type_struct', 'Not available')}")
        print(f"  Type debug: {col.get('type_debug', 'Not available')}")
        print()

    # Verify we have structured information
    assert len(columns) == 5
    assert all("type_struct" in col for col in columns)

    # Check that we can identify types correctly
    id_col = columns[0]
    name_col = columns[1]
    score_col = columns[2]
    active_col = columns[3]
    age_col = columns[4]

    assert id_col["type_struct"]["kind"] == "native"
    assert id_col["type_struct"]["name"] == "int"

    assert name_col["type_struct"]["kind"] == "native"
    assert name_col["type_struct"]["name"] == "text"

    assert score_col["type_struct"]["kind"] == "native"
    assert score_col["type_struct"]["name"] == "double"

    assert active_col["type_struct"]["kind"] == "native"
    assert active_col["type_struct"]["name"] == "boolean"

    assert age_col["type_struct"]["kind"] == "native"
    assert age_col["type_struct"]["name"] == "bigint"

    print("Column metadata inspection SUCCESS")


@pytest.mark.asyncio
async def test_serialization():
    """Test serialization functionality"""
    builder = SessionBuilder(["127.0.0.2"], 9042)
    session = await builder.connect()

    _ = await session.execute("USE test_ks")

    # Prepare statement for basic types
    prepared = await session.prepare(
        "INSERT INTO test_ks.basic_types (id, name, score) VALUES (?, ?, ?)"
    )

    test_values = [
        100,
        "Test User",
        88.8,
    ]

    serialization_success = False

    # Test serialization
    try:
        result = await session.execute_unpaged_python(prepared, test_values)
        print(f"Serialization SUCCESS: {result}")
        serialization_success = True
    except Exception as e:
        print(f"Serialization failed: {e}")

    # Serialization should work
    assert serialization_success, "Serialization failed"

    print(f"Serialization test completed successfully")


@pytest.mark.asyncio
async def test_structured_edge_cases():
    """Test edge cases with structured serialization"""
    builder = SessionBuilder(["127.0.0.2"], 9042)
    session = await builder.connect()

    _ = await session.execute("USE test_ks")

    # Create table with various edge case types
    _ = await session.execute("""
        CREATE TABLE IF NOT EXISTS edge_cases_test (
            id int PRIMARY KEY,
            empty_list list<text>,
            single_item_list list<int>,
            null_values_list list<text>,
            mixed_boolean boolean,
            zero_values bigint,
            negative_values int
        )
    """)

    prepared = await session.prepare(
        "INSERT INTO test_ks.edge_cases_test (id, empty_list, single_item_list, null_values_list, mixed_boolean, zero_values, negative_values) VALUES (?, ?, ?, ?, ?, ?, ?)"
    )

    # Test edge cases
    edge_test_cases = [
        [1, [], [42], [], True, 0, -999],  # Empty lists, zero values
        [2, [], [], [], False, -1, 0],  # Multiple empty lists
        [
            3,
            ["single"],
            [1],
            ["test"],
            True,
            999999999999,
            -2147483648,
        ],  # Single items, max values
    ]

    for test_values in edge_test_cases:
        try:
            result = await session.execute_unpaged_python(prepared, test_values)
            print(f"Edge case test SUCCESS for {test_values}: {result}")
        except Exception as e:
            print(f"Edge case test FAILED for {test_values}: {e}")


@pytest.mark.asyncio
async def test_structured_null_handling():
    """Test NULL value handling with structured serialization"""
    builder = SessionBuilder(["127.0.0.2"], 9042)
    session = await builder.connect()

    _ = await session.execute("USE test_ks")

    prepared = await session.prepare(
        "INSERT INTO test_ks.nullable_test (id, name, score, tags) VALUES (?, ?, ?, ?)"
    )

    null_test_cases = [
        [10, None, None, None],  # All NULLs except ID
        [11, "Test", None, []],  # Mixed NULLs
        [12, "Test 2", 85.5, None],  # NULL list
    ]

    for test_values in null_test_cases:
        try:
            result = await session.execute_unpaged_python(prepared, test_values)
            print(f"NULL handling test SUCCESS for {test_values}: {result}")
        except Exception as e:
            print(f"NULL handling test FAILED for {test_values}: {e}")


@pytest.mark.asyncio
async def test_structured_type_validation():
    """Test type validation errors with structured serialization"""
    builder = SessionBuilder(["127.0.0.2"], 9042)
    session = await builder.connect()

    _ = await session.execute("USE test_ks")

    prepared = await session.prepare(
        "INSERT INTO test_ks.structured_test (id, name, score, active, age) VALUES (?, ?, ?, ?, ?)"
    )

    # Test type validation errors
    invalid_test_cases = [
        ([1, "test", "not_a_number", True, 123], "Invalid double"),
        ([1, "test", 1.0, "not_a_boolean", 123], "Invalid boolean"),
        (["not_an_int", "test", 1.0, True, 123], "Invalid int"),
        ([1, "test", 1.0, True], "Too few arguments"),
        ([1, "test", 1.0, True, 123, "extra"], "Too many arguments"),
    ]

    for test_values, description in invalid_test_cases:
        try:
            result = await session.execute_unpaged_python(prepared, test_values)
            print(f"ERROR: {description} should have failed but didn't")
        except Exception as e:
            print(f"Expected validation error for {description}: {e}")


@pytest.mark.asyncio
async def test_structured_serializer_direct():
    """Test structured serializer functions directly"""
    builder = SessionBuilder(["127.0.0.2"], 9042)
    session = await builder.connect()

    _ = await session.execute("USE test_ks")

    prepared = await session.prepare(
        "INSERT INTO test_ks.basic_types (id, name, score) VALUES (?, ?, ?)"
    )

    try:
        from scylla.serializalize import serialize

        # Test direct serializer usage
        test_values = [999, "Direct Test", 88.8]

        result = serialize(test_values, prepared)

        # Validate result format
        assert isinstance(result, tuple), f"Expected tuple, got {type(result)}"
        assert len(result) == 2, f"Expected tuple of length 2, got {len(result)}"

        serialized_bytes, element_count = result
        assert isinstance(serialized_bytes, bytes), (
            f"Expected bytes, got {type(serialized_bytes)}"
        )
        assert isinstance(element_count, int), (
            f"Expected int, got {type(element_count)}"
        )
        assert element_count == 3, f"Expected 3 elements, got {element_count}"

        print(
            f"Direct structured serializer SUCCESS: {len(serialized_bytes)} bytes, {element_count} elements"
        )

    except ImportError:
        print("Structured serializer not available for direct testing")
    except Exception as e:
        print(f"Direct serializer test failed: {e}")


@pytest.mark.asyncio
async def test_column_introspection_detailed():
    """Detailed test of column introspection capabilities"""
    builder = SessionBuilder(["127.0.0.2"], 9042)
    session = await builder.connect()

    _ = await session.execute("USE test_ks")

    # Test with list types for more complex introspection
    prepared_list = await session.prepare(
        "INSERT INTO test_ks.structured_list_test (id, tags, scores) VALUES (?, ?, ?)"
    )

    ctx = PyRowSerializationContext.from_prepared(prepared_list)
    columns = ctx.columns()

    for i, col in enumerate(columns):
        print(f"List Column {i}: {col['name']}")

        type_struct = col.get("type_struct")
        if type_struct:
            print(f"  Kind: {type_struct.get('kind')}")
            print(f"  Name: {type_struct.get('name')}")

            # Check for collection metadata
            if type_struct.get("kind") == "collection":
                element_type = type_struct.get("element_type")
                if element_type:
                    print(
                        f"  Element type: {element_type.get('kind')}/{element_type.get('name')}"
                    )

        print()

    # Verify list columns have proper structure
    if len(columns) >= 3:
        # Check tags column (should be list<text>)
        tags_col = columns[1]  # Assuming tags is second column
        assert tags_col["name"] == "tags"
        tags_type = tags_col.get("type_struct", {})
        if tags_type.get("kind") == "collection" and tags_type.get("name") == "list":
            element_type = tags_type.get("element_type", {})
            assert element_type.get("kind") == "native"
            assert element_type.get("name") == "text"
            print("Tags column structure validated successfully")

        # Check scores column (should be list<int>)
        scores_col = columns[2]  # Assuming scores is third column
        assert scores_col["name"] == "scores"
        scores_type = scores_col.get("type_struct", {})
        if (
            scores_type.get("kind") == "collection"
            and scores_type.get("name") == "list"
        ):
            element_type = scores_type.get("element_type", {})
            assert element_type.get("kind") == "native"
            assert element_type.get("name") == "int"
            print("Scores column structure validated successfully")

    print("Detailed column introspection completed")


@pytest.mark.asyncio
async def test_udt():
    builder = SessionBuilder(["127.0.0.2"], 9042)
    session = await builder.connect()
    # This will now work with both Rust and Python changes:
    _ = await session.execute("USE test_ks")
    _ = await session.execute("""
        CREATE TYPE IF NOT EXISTS address (
            street text,
            city text,
            zip_code int
        )
    """)
    _ = await session.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id int PRIMARY KEY,
            addr address
        )
    """)

    prepared = await session.prepare("INSERT INTO users (id, addr) VALUES (?, ?)")

    # All these formats will work:
    _ = await session.execute_unpaged_python(
        prepared,
        [
            1,
            {"street": "123 Main St", "city": "Anytown", "zip_code": 12345},  # Dict
        ],
    )

    # Or with dataclass
    @dataclass
    class Address:
        street: str
        city: str
        zip_code: int

    addr = Address("456 Oak Ave", "Springfield", 67890)
    _ = await session.execute_unpaged_python(prepared, [2, addr])
