from typing import Any

import pytest
from scylla._rust.session_builder import SessionBuilder  # pyright: ignore[reportMissingModuleSource]

from scylla._rust.results import RowFactory, ColumnIterator  # pyright: ignore[reportMissingModuleSource]


@pytest.mark.asyncio
async def test_simple_deserialization():
    # 1. Connect
    builder = SessionBuilder(["127.0.0.2"], 9042)
    session = await builder.connect()

    # 2. Create keyspace & table
    await session.execute("""
        CREATE KEYSPACE IF NOT EXISTS testks
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
    """)

    await session.execute("""
                          CREATE TABLE IF NOT EXISTS testks.example_table (
                                                                              id int PRIMARY KEY,
                                                                              value_int int,
                                                                              value_text text
                          );
                          """)

    # 3. Insert test rows
    await session.execute("""
                          INSERT INTO testks.example_table (id, value_int, value_text)
                          VALUES (1, 42, 'hello');
                          """)

    await session.execute("""
                          INSERT INTO testks.example_table (id, value_int, value_text)
                          VALUES (2, 99, 'world');
                          """)

    # 4. Query the data
    result = await session.execute("SELECT * FROM testks.example_table")

    # 5. Print CQL values (should not throw)
    rows = result.create_rows_result()
    for row in rows:
        print(row)


@pytest.mark.asyncio
async def test_list_deserialization():
    # 1. Connect
    builder = SessionBuilder(["127.0.0.2"], 9042)
    session = await builder.connect()

    # 3. Create a table with complex CQL types
    await session.execute("""
                          CREATE TABLE IF NOT EXISTS testks.complex_table2 (
                                                                              id int PRIMARY KEY,
                                                                              name text,
                                                                              scores list<int>,
                          );
                          """)

    # 4. Insert complex rows
    await session.execute("""
                          INSERT INTO testks.complex_table2
                              (id, name, scores)
                          VALUES (
                                     1,
                                     'Alice',
                                     [10, 20, 30]
                                 );
                          """)

    await session.execute("""
                          INSERT INTO testks.complex_table2
                              (id, name, scores)
                          VALUES (
                                     2,
                                     'Bob',
                                     [100, 200]
                                 );
                          """)

    # 5. Query the data
    result = await session.execute("SELECT * FROM testks.complex_table2")

    rows = result.create_rows_result()

    for row in rows:
        assert isinstance(row, dict)


@pytest.mark.asyncio
async def test_udt_deserialization():
    # 1. Connect
    builder = SessionBuilder(["127.0.0.2"], 9042)
    session = await builder.connect()

    # 2. Create UDT + table
    await session.execute("""
        CREATE TYPE IF NOT EXISTS testks.address (
            street text,
            number int
        );
    """)

    await session.execute("""
                          CREATE TABLE IF NOT EXISTS testks.persons_udt (
                                                                            id int PRIMARY KEY,
                                                                            name text,
                                                                            addr address
                          );
                          """)

    # 3. Insert 2 rows with UDT values
    await session.execute("""
                          INSERT INTO testks.persons_udt (id, name, addr)
                          VALUES (
                                     1,
                                     'Alice',
                                     { street: 'Main St', number: 10 }
                                 );
                          """)

    await session.execute("""
                          INSERT INTO testks.persons_udt (id, name, addr)
                          VALUES (
                                     2,
                                     'Bob',
                                     { street: 'Oak Ave', number: 42 }
                                 );
                          """)

    # 4. Query + convert rows using your deserializer
    result = await session.execute("SELECT * FROM testks.persons_udt")
    rows = result.create_rows_result()

    # 5. Verify Python structure
    row_list = list(rows)

    assert isinstance(row_list[0], dict)
    assert isinstance(row_list[1], dict)

    # Check first row
    assert row_list[0]["name"] == "Alice"
    assert row_list[0]["addr"]["street"] == "Main St"
    assert row_list[0]["addr"]["number"] == 10

    # Check second row
    assert row_list[1]["name"] == "Bob"
    assert row_list[1]["addr"]["street"] == "Oak Ave"
    assert row_list[1]["addr"]["number"] == 42


@pytest.mark.asyncio
async def test_list_udt_deserialization():
    # 1. Connect
    builder = SessionBuilder(["127.0.0.2"], 9042)
    session = await builder.connect()

    # 2. Define UDT + table with list<address>
    await session.execute("""
        CREATE TYPE IF NOT EXISTS testks.address (
            street text,
            number int
        );
    """)

    await session.execute("""
                          CREATE TABLE IF NOT EXISTS testks.people_with_addresses (
                                                                                      id   int,
                                                                                      name text,
                                                                                      addrs list<frozen<address>>,
                                                                                      PRIMARY KEY (id, name)
                              );
                          """)

    # 3. Insert multiple rows with list<udt>
    rows_to_insert = 12  # 10–15 as requested
    for i in range(rows_to_insert):
        await session.execute(f"""
            INSERT INTO testks.people_with_addresses (id, name, addrs)
            VALUES (
                0,
                'User{i}',
                [
                    {{ street: 'A-Street-{i}', number: {i * 10} }},
                    {{ street: 'B-Street-{i}', number: {i * 10 + 1} }}
                ]
            );
        """)

    # 4. Query + deserialize
    result = await session.execute("""
                                   SELECT * FROM testks.people_with_addresses
                                    WHERE id = 0
                                   ORDER BY name ASC
                                   """)
    rows = result.create_rows_result()
    row_list = list(rows)

    # 5. Assertions — verify all rows returned + structure is correct
    assert len(row_list) == rows_to_insert

    for i, row in enumerate(row_list):
        print(row)
        assert isinstance(row["addrs"], list)


@pytest.mark.asyncio
async def test_custom_row_factory_transforms_rows():
    # Define a simple Python class for output rows
    class UserRow:
        def __init__(self, id: int, name: str, scores: list[int]):
            self.id = id
            self.name = name
            self.scores = scores

    # A valid RowFactory implementation
    class UserFactory(RowFactory):
        cls = UserRow

        def build(self, column_iterator: ColumnIterator) -> Any:
            id = 0
            name = "Some"
            scores = []
            for col in column_iterator:
                if col.column_name == "id":
                    id = int(col.value)  # type: ignore[arg-type]
                elif col.column_name == "name":
                    name = str(col.value)  # type: ignore[arg-type]
                elif col.column_name == "scores":
                    scores = list(col.value)  # type: ignore[arg-type]
            return UserRow(id, name, scores)

    # Connect
    builder = SessionBuilder(["127.0.0.2"], 9042)
    session = await builder.connect()

    # Create table
    await session.execute("""
                          CREATE TABLE IF NOT EXISTS testks.custom_factory_table (
                                                                                     id int PRIMARY KEY,
                                                                                     name text,
                                                                                     scores list<int>
                          );
                          """)

    # Insert example rows
    await session.execute("""
                          INSERT INTO testks.custom_factory_table (id, name, scores)
                          VALUES (1, 'Alice', [1, 2, 3])
                          """)
    await session.execute("""
                          INSERT INTO testks.custom_factory_table (id, name, scores)
                          VALUES (2, 'Bob', [5, 10])
                          """)

    # Query
    result = await session.execute("SELECT * FROM testks.custom_factory_table")

    # Apply the custom row factory
    rows = result.create_rows_result(UserFactory())

    # Iterate and validate
    collected = list(rows)

    assert len(collected) == 2
    assert all(isinstance(r, UserRow) for r in collected)

    # Check actual content transformation
    alice = next(r for r in collected if r.id == 1)
    assert alice.name == "Alice"
    assert alice.scores == [1, 2, 3]

    bob = next(r for r in collected if r.id == 2)
    assert bob.name == "Bob"
    assert bob.scores == [5, 10]
