from typing import Any, Callable, Awaitable, AsyncGenerator

import pytest
import uuid
import ipaddress
import datetime
from decimal import Decimal
from datetime import time
from dateutil.relativedelta import relativedelta
import pytest_asyncio
from scylla._rust.session import Session  # pyright: ignore[reportMissingModuleSource]
from scylla._rust.session_builder import SessionBuilder  # pyright: ignore[reportMissingModuleSource]

from scylla._rust.results import RowFactory, ColumnIterator  # pyright: ignore[reportMissingModuleSource]


async def set_up() -> Session:
    builder = SessionBuilder(["127.0.0.2"], 9042)
    session = await builder.connect()

    # 2. Create keyspace & table
    await session.execute("""
            CREATE KEYSPACE IF NOT EXISTS testks
            WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1};
        """)

    await session.execute("USE testks")

    return session


@pytest_asyncio.fixture(scope="module")
async def session():
    session = await set_up()
    yield session
    await session.execute("DROP KEYSPACE testks")


TableFactory = Callable[[str, str], Awaitable[str]]


@pytest_asyncio.fixture
async def table_factory(session: Session) -> AsyncGenerator[TableFactory, None]:
    created_tables: list[str] = []

    async def create_table(schema: str, name: str) -> str:
        await session.execute(f"CREATE TABLE IF NOT EXISTS {name} ({schema});")
        created_tables.append(name)
        return name

    yield create_table

    for table in created_tables:
        await session.execute(f"DROP TABLE IF EXISTS {table};")


# Verifies that iter_rows() returns an iterator yielding row dictionaries
@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_rows_result_is_iterator_and_dicts(session: Session, table_factory: TableFactory):
    table = await table_factory(
        "id int PRIMARY KEY, x int",
        "rows_result_basic_table",
    )

    await session.execute(f"INSERT INTO {table} (id, x) VALUES (1, 10);")
    await session.execute(f"INSERT INTO {table} (id, x) VALUES (2, 20);")

    result = await session.execute(f"SELECT * FROM {table}")
    rows = result.iter_rows()

    # rows_result should be an iterator
    assert iter(rows) is rows

    # iterating should yield dicts
    first = next(rows)
    assert isinstance(first, dict)

    assert "id" in first
    assert "x" in first


# Verifies correct deserialization of CQL text values, including Unicode
@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_text_deserialization(session: Session, table_factory: TableFactory):
    table = await table_factory(
        "id int PRIMARY KEY, t text",
        "text_table",
    )

    v1 = "hello"
    v2 = "Zażółć gęślą jaźń 🚀"

    await session.execute(f"INSERT INTO {table} (id, t) VALUES (1, '{v1}');")
    await session.execute(f"INSERT INTO {table} (id, t) VALUES (2, '{v2}');")

    result = await session.execute(f"SELECT * FROM {table}")
    rows = list(result.iter_rows())

    assert len(rows) == 2

    row1 = next(r for r in rows if r["id"] == 1)
    row2 = next(r for r in rows if r["id"] == 2)

    t1 = row1["t"]
    t2 = row2["t"]

    assert isinstance(t1, str)
    assert isinstance(t2, str)

    assert t1 == v1
    assert t2 == v2


# Verifies correct deserialization of CQL float values
@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_float_deserialization(session: Session, table_factory: TableFactory):
    table = await table_factory(
        "id int PRIMARY KEY, f float",
        "float_table",
    )

    v1 = 1.25
    v2 = -42.5

    await session.execute(f"INSERT INTO {table} (id, f) VALUES (1, {v1});")
    await session.execute(f"INSERT INTO {table} (id, f) VALUES (2, {v2});")

    result = await session.execute(f"SELECT * FROM {table}")
    rows = list(result.iter_rows())

    assert len(rows) == 2

    row1 = next(r for r in rows if r["id"] == 1)
    row2 = next(r for r in rows if r["id"] == 2)

    f1 = row1["f"]
    f2 = row2["f"]

    assert isinstance(f1, float)
    assert isinstance(f2, float)

    assert f1 == pytest.approx(v1)  # pyright: ignore[reportUnknownMemberType]
    assert f2 == pytest.approx(v2)  # pyright: ignore[reportUnknownMemberType]


# Verifies correct deserialization of CQL double values
@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_double_deserialization(session: Session, table_factory: TableFactory):
    table = await table_factory(
        "id int PRIMARY KEY, d double",
        "double_table",
    )

    v1 = 3.141592653589793
    v2 = -0.00000012345

    await session.execute(f"INSERT INTO {table} (id, d) VALUES (1, {v1});")
    await session.execute(f"INSERT INTO {table} (id, d) VALUES (2, {v2});")

    result = await session.execute(f"SELECT * FROM {table}")
    rows = list(result.iter_rows())

    assert len(rows) == 2

    row1 = next(r for r in rows if r["id"] == 1)
    row2 = next(r for r in rows if r["id"] == 2)

    d1 = row1["d"]
    d2 = row2["d"]

    assert isinstance(d1, float)
    assert isinstance(d2, float)

    assert d1 == pytest.approx(v1)  # pyright: ignore[reportUnknownMemberType]
    assert d2 == pytest.approx(v2)  # pyright: ignore[reportUnknownMemberType]


# Verifies correct deserialization of CQL list<int> into Python lists
@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_list_deserialization(session: Session, table_factory: TableFactory):
    table = await table_factory(
        "id int PRIMARY KEY, list list<int>",
        "nested_list_table",
    )

    v1 = [1, 2, 3]
    v2 = [35, 53, 15, 12]

    await session.execute(f"INSERT INTO {table} (id, list) VALUES (1, {v1});")
    await session.execute(f"INSERT INTO {table} (id, list) VALUES (2, {v2});")

    result = await session.execute(f"SELECT * FROM {table}")
    rows = list(result.iter_rows())

    assert len(rows) == 2

    row1 = next(r for r in rows if r["id"] == 1)
    row2 = next(r for r in rows if r["id"] == 2)

    list1 = row1["list"]
    list2 = row2["list"]

    # Type checks
    assert isinstance(list1, list)
    assert isinstance(list2, list)
    # Value checks
    assert list1 == v1
    assert list2 == v2


# Verifies correct deserialization of nested CQL list<frozen<list<int>>
@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_nested_list_deserialization(session: Session, table_factory: TableFactory):
    table = await table_factory(
        "id int PRIMARY KEY, nested list<frozen<list<int>>>",
        "nested_list_table",
    )

    v1 = [[1, 2, 3], [4, 5], [3, 2, 1]]
    v2 = [[10], [20, 30, 40], [3, 2, 1], [3, 2, 1]]

    await session.execute(f"INSERT INTO {table} (id, nested) VALUES (1, {v1});")
    await session.execute(f"INSERT INTO {table} (id, nested) VALUES (2, {v2});")

    result = await session.execute(f"SELECT * FROM {table}")
    rows = list(result.iter_rows())

    assert len(rows) == 2

    row1 = next(r for r in rows if r["id"] == 1)
    row2 = next(r for r in rows if r["id"] == 2)

    nested1 = row1["nested"]
    nested2 = row2["nested"]

    # Type checks
    assert isinstance(nested1, list)
    assert isinstance(nested2, list)

    for inner in nested1:  # pyright: ignore[reportUnknownVariableType]
        assert isinstance(inner, list)

    for inner in nested2:  # pyright: ignore[reportUnknownVariableType]
        assert isinstance(inner, list)

    # Value checks
    assert nested1 == v1
    assert nested2 == v2


# Verifies correct deserialization of CQL UDT into Python dictionaries
@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_udt_deserialization(session: Session, table_factory: TableFactory):
    # Create UDT
    await session.execute("""
        CREATE TYPE IF NOT EXISTS testks.address (
            street text,
            number int
        );
    """)

    table = await table_factory("id int PRIMARY KEY, d address", "udt_table")

    d1 = {"street": "Main St", "number": 10}
    d2 = {"street": "Oak Ave", "number": 42}

    d1_literal = "{ street: 'Main St', number: 10 }"
    d2_literal = "{ street: 'Oak Ave', number: 42 }"

    await session.execute(f"INSERT INTO {table} (id, d) VALUES (1, {d1_literal});")
    await session.execute(f"INSERT INTO {table} (id, d) VALUES (2, {d2_literal});")

    result = await session.execute(f"SELECT * FROM {table}")
    rows = list(result.iter_rows())

    assert len(rows) == 2

    row1 = next(r for r in rows if r["id"] == 1)
    row2 = next(r for r in rows if r["id"] == 2)

    d1_out = row1["d"]
    d2_out = row2["d"]

    # Type checks
    assert isinstance(d1_out, dict)
    assert isinstance(d2_out, dict)

    # Field checks
    assert d1_out["street"] == d1["street"]
    assert d1_out["number"] == d1["number"]

    assert d2_out["street"] == d2["street"]
    assert d2_out["number"] == d2["number"]


# Verifies correct deserialization of list<frozen<UDT>> across multiple rows
@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_list_udt_deserialization(session: Session, table_factory: TableFactory):
    await session.execute("""
        CREATE TYPE IF NOT EXISTS testks.address (
            street text,
            number int
        );
    """)

    table = await table_factory("id int, name text, addrs list<frozen<address>>, PRIMARY KEY (id, name)", "nested_udt")

    # 3. Insert multiple rows with list<udt>
    rows_to_insert = 9  # 10–15 as requested
    for i in range(rows_to_insert):
        await session.execute(f"""
            INSERT INTO {table} (id, name, addrs)
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
    result = await session.execute(f"SELECT * FROM {table} WHERE id = 0 ORDER BY name ASC")
    rows = result.iter_rows()
    row_list = list(rows)

    # 5. Assertions — verify all rows returned + structure is correct
    assert len(row_list) == rows_to_insert

    for i, row in enumerate(row_list):
        assert isinstance(row["addrs"], list)
        assert row["name"] == f"User{i}"
        assert row["addrs"][0]["street"] == f"A-Street-{i}"
        assert row["addrs"][0]["number"] == i * 10
        assert row["addrs"][1]["street"] == f"B-Street-{i}"
        assert row["addrs"][1]["number"] == i * 10 + 1


# Verifies that a custom RowFactory can transform rows into user-defined Python objects
@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_custom_row_factory_transforms_rows(session: Session, table_factory: TableFactory):
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
            return UserRow(id, name, scores)  # type: ignore[arg-type]

    # Create table
    table = await table_factory("id int PRIMARY KEY, name text, scores list<int>", "example_table")

    await session.execute(f"INSERT INTO {table} (id, name, scores) VALUES (1, 'Alice', [1, 2, 3]);")
    await session.execute(f"INSERT INTO {table} (id, name, scores) VALUES (2, 'Bob', [5, 10]);")

    # Query
    result = await session.execute(f"SELECT * FROM {table}")

    # Apply the custom row factory
    rows = result.iter_rows(UserFactory())

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


# Verifies correct deserialization of CQL uuid into Python UUID
@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_uuid_deserialization(session: Session, table_factory: TableFactory):
    table = await table_factory("id int PRIMARY KEY, uid uuid", "uuid_table")

    # 1. Generate UUIDs
    uid1 = uuid.uuid4()
    uid2 = uuid.uuid4()

    # 2. Insert them
    await session.execute(f"INSERT INTO {table} (id, uid) VALUES (1, {uid1});")
    await session.execute(f"INSERT INTO {table} (id, uid) VALUES (2, {uid2});")

    # 3. Query result
    result = await session.execute(f"SELECT * FROM {table}")
    rows = list(result.iter_rows())

    assert len(rows) == 2

    row1 = next(r for r in rows if r["id"] == 1)
    row2 = next(r for r in rows if r["id"] == 2)

    # Type checks
    import uuid as _uuid

    assert isinstance(row1["uid"], _uuid.UUID)
    assert isinstance(row2["uid"], _uuid.UUID)

    # Value checks
    assert row1["uid"] == uid1
    assert row2["uid"] == uid2


# Verifies correct deserialization of CQL inet into IPv4 and IPv6 address objects
@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_inet_deserialization(session: Session, table_factory: TableFactory):
    table = await table_factory("id int PRIMARY KEY, ip inet", "inet_table")

    # 1. Prepare some IPv4 and IPv6 addresses
    ip1 = ipaddress.ip_address("192.168.1.10")
    ip2 = ipaddress.ip_address("2001:db8::1")

    # 2. Insert them into the database
    await session.execute(f"INSERT INTO {table} (id, ip) VALUES (1, '{ip1}');")
    await session.execute(f"INSERT INTO {table} (id, ip) VALUES (2, '{ip2}');")

    # 3. Query results
    result = await session.execute(f"SELECT * FROM {table}")
    rows = list(result.iter_rows())

    assert len(rows) == 2

    row1 = next(r for r in rows if r["id"] == 1)
    row2 = next(r for r in rows if r["id"] == 2)

    # 4. Type checks
    assert isinstance(row1["ip"], ipaddress.IPv4Address)
    assert isinstance(row2["ip"], ipaddress.IPv6Address)

    # 5. Value checks
    assert row1["ip"] == ip1
    assert row2["ip"] == ip2


# Verifies correct deserialization of CQL timeuuid into version-1 UUID
@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_timeuuid_deserialization(session: Session, table_factory: TableFactory):
    table = await table_factory("id int PRIMARY KEY, tid timeuuid", "timeuuid_table")

    # Generate time-based UUID (v1)
    tid1 = uuid.uuid1()
    tid2 = uuid.uuid1()

    await session.execute(f"INSERT INTO {table} (id, tid) VALUES (1, {tid1});")
    await session.execute(f"INSERT INTO {table} (id, tid) VALUES (2, {tid2});")

    result = await session.execute(f"SELECT * FROM {table}")
    rows = list(result.iter_rows())

    assert len(rows) == 2

    row1 = next(r for r in rows if r["id"] == 1)
    row2 = next(r for r in rows if r["id"] == 2)

    # Type checks
    assert isinstance(row1["tid"], uuid.UUID)
    assert isinstance(row2["tid"], uuid.UUID)

    # Must be time-based UUIDs (version 1)
    assert row1["tid"].version == 1
    assert row2["tid"].version == 1

    # Value round-trip correctness
    assert row1["tid"] == tid1
    assert row2["tid"] == tid2


# Verifies correct deserialization of CQL date into Python date objects
@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_date_deserialization(session: Session, table_factory: TableFactory):
    table = await table_factory("id int PRIMARY KEY, d date", "date_table")

    # Prepare two dates: one recent, one historic
    d1 = datetime.date(2024, 5, 10)
    d2 = datetime.date(1999, 12, 31)

    # Insert using ISO strings (Cassandra accepts YYYY-MM-DD)
    await session.execute(f"INSERT INTO {table} (id, d) VALUES (1, '{d1.isoformat()}');")
    await session.execute(f"INSERT INTO {table} (id, d) VALUES (2, '{d2.isoformat()}');")

    # Read back
    result = await session.execute(f"SELECT * FROM {table}")
    rows = list(result.iter_rows())

    assert len(rows) == 2

    row1 = next(r for r in rows if r["id"] == 1)
    row2 = next(r for r in rows if r["id"] == 2)

    # Type checks
    assert isinstance(row1["d"], datetime.date)
    assert isinstance(row2["d"], datetime.date)

    # Value correctness
    assert row1["d"] == d1
    assert row2["d"] == d2

    # Additional sanity checks (same ordering)
    assert row1["d"].isoformat() == "2024-05-10"
    assert row2["d"].isoformat() == "1999-12-31"


# Verifies correct deserialization of CQL ascii into Python strings
@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_ascii_deserialization(session: Session, table_factory: TableFactory):
    table = await table_factory("id int PRIMARY KEY, txt ascii", "ascii_table")

    # ASCII-only values
    v1 = "HelloASCII"
    v2 = "Test123_ABC"

    # Insert values
    await session.execute(f"INSERT INTO {table} (id, txt) VALUES (1, '{v1}');")
    await session.execute(f"INSERT INTO {table} (id, txt) VALUES (2, '{v2}');")

    # Read back
    result = await session.execute(f"SELECT * FROM {table}")
    rows = list(result.iter_rows())

    assert len(rows) == 2

    row1 = next(r for r in rows if r["id"] == 1)
    row2 = next(r for r in rows if r["id"] == 2)

    # Type checks
    assert isinstance(row1["txt"], str)
    assert isinstance(row2["txt"], str)

    # Value correctness
    assert row1["txt"] == v1
    assert row2["txt"] == v2

    # Ensures no accidental unicode alterations
    assert row1["txt"].isascii()
    assert row2["txt"].isascii()


# Verifies exact round-trip deserialization of CQL decimal into Python Decimal
@pytest.mark.asyncio
@pytest.mark.requires_db
@pytest.mark.parametrize(
    "row_id,value",
    [
        (1, Decimal("0")),
        (2, Decimal("0.0")),
        (3, Decimal("-0")),
        (4, Decimal("123.456")),
        (5, Decimal("-7890.00123")),
        (6, Decimal("123456789012345678901234567890.123456789")),
        (7, Decimal("-999999999999999999999999999999999999.9999")),
        (8, Decimal("0.00000000000000000001")),
        (9, Decimal("42.000000")),
        (10, Decimal("617283694")),
        (11, Decimal("617283694.4324")),
    ],
)
async def test_decimal_deserialization(
    session: Session,
    table_factory: TableFactory,
    row_id: int,
    value: str,
):
    table = await table_factory(
        "id int PRIMARY KEY, price decimal",
        "decimal_parametrized_table",
    )

    await session.execute(f"INSERT INTO {table} (id, price) VALUES ({row_id}, {value});")

    result = await session.execute(f"SELECT * FROM {table} WHERE id = {row_id}")
    row = next(result.iter_rows())

    assert isinstance(row["price"], Decimal)
    assert row["price"] == value


# Verifies correct deserialization of CQL varint into Python int without precision loss
@pytest.mark.asyncio
@pytest.mark.requires_db
@pytest.mark.parametrize(
    "row_id,value",
    [
        (1, 0),
        (2, 1),
        (3, -1),
        (4, 42),
        (5, -42),
        (6, 2**63 - 1),
        (7, -(2**63)),
        (8, 12345678901234567890),
        (9, -12345678901234567890),
        (10, 123456789012345678901234567890),
        (11, -99999999999999999999999999999999999999),
        (12, 10**100),
        (13, -(10**100)),
    ],
)
async def test_varint_deserialization_parametrized(
    session: Session,
    table_factory: TableFactory,
    row_id: int,
    value: str,
):
    table = await table_factory(
        "id int PRIMARY KEY, val varint",
        "varint_parametrized_table",
    )

    await session.execute(f"INSERT INTO {table} (id, val) VALUES ({row_id}, {value});")

    result = await session.execute(f"SELECT * FROM {table} WHERE id = {row_id}")
    row = next(result.iter_rows())

    # Type check
    assert isinstance(row["val"], int)

    # Exact round-trip correctness
    assert row["val"] == value


# Verifies correct deserialization of CQL timestamp into timezone-aware datetime
@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_timestamp_deserialization(session: Session, table_factory: TableFactory):
    table = await table_factory("id int PRIMARY KEY, ts timestamp", "timestamp_table")

    dt1 = datetime.datetime(2024, 5, 10, 12, 30, 45, 123000, tzinfo=datetime.timezone.utc)
    dt2 = datetime.datetime(1999, 12, 31, 23, 59, 59, 0, tzinfo=datetime.timezone.utc)

    # Insert using epoch milliseconds
    ts1 = int(dt1.timestamp() * 1000)
    ts2 = int(dt2.timestamp() * 1000)

    await session.execute(f"INSERT INTO {table} (id, ts) VALUES (1, {ts1});")
    await session.execute(f"INSERT INTO {table} (id, ts) VALUES (2, {ts2});")

    rows = list((await session.execute(f"SELECT * FROM {table}")).iter_rows())

    r1 = next(r for r in rows if r["id"] == 1)
    r2 = next(r for r in rows if r["id"] == 2)

    assert r1["ts"] == dt1
    assert r2["ts"] == dt2


# Verifies correct deserialization of CQL time into Python time objects
@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_time_deserialization(session: Session, table_factory: TableFactory):
    table = await table_factory("id int PRIMARY KEY, t time", "time_table")

    # Time value: 1 hour, 30 minutes, 45 seconds, 500 milliseconds
    t1 = time(1, 30, 45, 500000)  # 1:30:45.500
    t2 = time(10, 15, 5, 123000)  # 10:15:05.123

    # Insert into CQL table
    await session.execute(f"INSERT INTO {table} (id, t) VALUES (1, '{t1.isoformat()}');")
    await session.execute(f"INSERT INTO {table} (id, t) VALUES (2, '{t2.isoformat()}');")

    result = await session.execute(f"SELECT * FROM {table}")
    rows = list(result.iter_rows())

    assert len(rows) == 2

    row1 = next(r for r in rows if r["id"] == 1)
    row2 = next(r for r in rows if r["id"] == 2)

    # Type checks
    assert isinstance(row1["t"], time)
    assert isinstance(row2["t"], time)

    # Value checks
    assert row1["t"] == t1
    assert row2["t"] == t2


# Verifies correct deserialization of CQL duration into relativedelta
@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_duration_deserialization(session: Session, table_factory: TableFactory):
    table = await table_factory("id int PRIMARY KEY, d duration", "duration_table")

    # CQL duration representation:
    # <months>mo <days>d <nanos>ns
    d1 = relativedelta(months=2, days=5, microseconds=36)
    d2 = relativedelta(months=11, days=0, microseconds=123_456)

    # Cassandra duration literals look like: '2mo5d3600000000000ns'
    d1_literal = "2mo5d36000ns"
    d2_literal = "11mo0d123456000ns"

    # Insert into CQL
    await session.execute(f"INSERT INTO {table} (id, d) VALUES (1, {d1_literal});")
    await session.execute(f"INSERT INTO {table} (id, d) VALUES (2, {d2_literal});")

    # Retrieve
    result = await session.execute(f"SELECT * FROM {table}")
    rows = list(result.iter_rows())

    assert len(rows) == 2

    row1 = next(r for r in rows if r["id"] == 1)
    row2 = next(r for r in rows if r["id"] == 2)

    d1_out = row1["d"]
    d2_out = row2["d"]

    # Type checks
    assert isinstance(d1_out, relativedelta)
    assert isinstance(d2_out, relativedelta)

    assert d1_out == d1
    assert d2_out == d2


# Verifies correct deserialization of CQL map<text, int> into Python dict
@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_map_text_int_deserialization(session: Session, table_factory: TableFactory):
    table = await table_factory("id int PRIMARY KEY, m map<text, int>", "map_text_int_table")

    v1 = {"a": 1, "b": 2}
    v2 = {"x": 10, "y": 20, "z": 30}

    # Insert using CQL map literal syntax: { 'a': 1, 'b': 2 }
    await session.execute(f"INSERT INTO {table} (id, m) VALUES (1, {{'a':1,'b':2}});")
    await session.execute(f"INSERT INTO {table} (id, m) VALUES (2, {{'x':10,'y':20,'z':30}});")

    rows = list((await session.execute(f"SELECT * FROM {table}")).iter_rows())

    r1 = next(r for r in rows if r["id"] == 1)
    r2 = next(r for r in rows if r["id"] == 2)

    assert isinstance(r1["m"], dict)
    assert isinstance(r2["m"], dict)

    assert r1["m"] == v1
    assert r2["m"] == v2


# Verifies correct deserialization of CQL map<text, list<int>> into nested Python structures
@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_map_text_list_int_deserialization(session: Session, table_factory: TableFactory):
    table = await table_factory("id int PRIMARY KEY, m map<text, frozen<list<int>>>", "map_text_list_int_table")

    v1 = {"a": [1, 2, 3], "b": [10]}
    v2 = {"x": [], "y": [5, 5, 5]}

    await session.execute(f"INSERT INTO {table} (id, m) VALUES (1, {{'a':[1,2,3], 'b':[10]}});")
    await session.execute(f"INSERT INTO {table} (id, m) VALUES (2, {{'x':[], 'y':[5,5,5]}});")

    rows = list((await session.execute(f"SELECT * FROM {table}")).iter_rows())

    r1 = next(r for r in rows if r["id"] == 1)
    r2 = next(r for r in rows if r["id"] == 2)

    assert isinstance(r1["m"], dict)
    assert isinstance(r2["m"], dict)

    assert r1["m"] == v1
    assert r2["m"] == v2


# Verifies correct deserialization of CQL set<int> into Python set
@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_set_int_deserialization(session: Session, table_factory: TableFactory):
    table = await table_factory("id int PRIMARY KEY, s set<int>", "set_int_table")

    v1 = {1, 2, 3}
    v2 = {10, 20}

    await session.execute(f"INSERT INTO {table} (id, s) VALUES (1, { {1, 2, 3} });")
    await session.execute(f"INSERT INTO {table} (id, s) VALUES (2, { {10, 20} });")

    rows = list((await session.execute(f"SELECT * FROM {table}")).iter_rows())

    r1 = next(r for r in rows if r["id"] == 1)
    r2 = next(r for r in rows if r["id"] == 2)

    assert isinstance(r1["s"], set)
    assert isinstance(r2["s"], set)

    assert r1["s"] == v1
    assert r2["s"] == v2


# Verifies correct deserialization of 3-element CQL tuple into Python tuple
@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_tuple_3_deserialization(session: Session, table_factory: TableFactory):
    table = await table_factory("id int PRIMARY KEY, t tuple<int, text, boolean>", "tuple_3_table")

    v1 = (1, "hello", True)
    v2 = (42, "world", False)

    await session.execute(f"INSERT INTO {table} (id, t) VALUES (1, ({v1[0]}, '{v1[1]}', {str(v1[2]).lower()}));")
    await session.execute(f"INSERT INTO {table} (id, t) VALUES (2, ({v2[0]}, '{v2[1]}', {str(v2[2]).lower()}));")

    rows = list((await session.execute(f"SELECT * FROM {table}")).iter_rows())

    r1 = next(r for r in rows if r["id"] == 1)
    r2 = next(r for r in rows if r["id"] == 2)

    assert isinstance(r1["t"], tuple)
    assert isinstance(r2["t"], tuple)

    assert r1["t"] == v1
    assert r2["t"] == v2


# Verifies correct deserialization of large (10-element) CQL tuple into Python tuple
@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_tuple_10_deserialization(session: Session, table_factory: TableFactory):
    table = await table_factory(
        """
        id int PRIMARY KEY,
        t tuple<int, text, double, boolean, bigint,
                 int, text, boolean, double, int>
        """,
        "tuple_10_table",
    )

    v1 = (1, "x", 3.14, True, 999, 10, "abc", False, -1.25, 777)

    v2 = (2, "y", -2.5, False, -555, 20, "zzz", True, 42.42, 123456)

    def fmt(v: Any) -> str:
        return str(v).lower() if isinstance(v, bool) else v

    await session.execute(
        f"INSERT INTO {table} (id, t) VALUES "
        f"(1, ({fmt(v1[0])}, '{v1[1]}', {v1[2]}, {fmt(v1[3])}, {v1[4]}, "
        f"{v1[5]}, '{v1[6]}', {fmt(v1[7])}, {v1[8]}, {v1[9]}));"
    )

    await session.execute(
        f"INSERT INTO {table} (id, t) VALUES "
        f"(2, ({fmt(v2[0])}, '{v2[1]}', {v2[2]}, {fmt(v2[3])}, {v2[4]}, "
        f"{v2[5]}, '{v2[6]}', {fmt(v2[7])}, {v2[8]}, {v2[9]}));"
    )

    rows = list((await session.execute(f"SELECT * FROM {table}")).iter_rows())

    r1 = next(r for r in rows if r["id"] == 1)
    r2 = next(r for r in rows if r["id"] == 2)

    assert isinstance(r1["t"], tuple)
    assert isinstance(r2["t"], tuple)

    assert r1["t"] == v1
    assert r2["t"] == v2


# Verifies correct deserialization of fixed-size CQL vector<float, 4> into Python list
@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_vector_4_deserialization(session: Session, table_factory: TableFactory):
    table = await table_factory("id int PRIMARY KEY, v vector<float, 4>", "vec4_table")

    v1 = [1.0, 2.5, -3.0, 4.25]
    v2 = [-1.0, 0.0, 0.5, 99.9]

    await session.execute(f"INSERT INTO {table} (id, v) VALUES (1, {v1});")
    await session.execute(f"INSERT INTO {table} (id, v) VALUES (2, {v2});")

    rows = list((await session.execute(f"SELECT * FROM {table}")).iter_rows())

    r1 = next(r for r in rows if r["id"] == 1)
    r2 = next(r for r in rows if r["id"] == 2)

    assert isinstance(r1["v"], list)
    assert isinstance(r2["v"], list)

    assert r1["v"] == pytest.approx(v1)  # pyright: ignore[reportUnknownMemberType]
    assert r2["v"] == pytest.approx(v2)  # pyright: ignore[reportUnknownMemberType]


# Verifies correct handling of NULL values in CQL set<int>
@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_set_int_null_deserialization(session: Session, table_factory: TableFactory):
    table = await table_factory("id int PRIMARY KEY, s set<int>", "set_int_null_table")

    v1 = {1, 2, 3}

    await session.execute(f"INSERT INTO {table} (id, s) VALUES (1, { {1, 2, 3} });")
    await session.execute(f"INSERT INTO {table} (id, s) VALUES (2, NULL);")

    rows = list((await session.execute(f"SELECT * FROM {table}")).iter_rows())

    r1 = next(r for r in rows if r["id"] == 1)
    r2 = next(r for r in rows if r["id"] == 2)

    assert isinstance(r1["s"], set)
    assert r2["s"] is None

    assert r1["s"] == v1


# Verifies correct deserialization of CQL blob into Python bytes, including empty blobs
@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_blob_deserialization(session: Session, table_factory: TableFactory):
    table = await table_factory("id int PRIMARY KEY, b blob", "blob_table")

    # Test values
    b1 = b"\x00\x01\x02\x03\xff"
    b2 = b"hello world"
    b3 = b""  # empty blob edge case

    # Cassandra blob literals are hex-encoded with 0x prefix
    b1_literal = "0x00010203ff"
    b2_literal = "0x68656c6c6f20776f726c64"
    b3_literal = "0x"

    # Insert
    await session.execute(f"INSERT INTO {table} (id, b) VALUES (1, {b1_literal});")
    await session.execute(f"INSERT INTO {table} (id, b) VALUES (2, {b2_literal});")
    await session.execute(f"INSERT INTO {table} (id, b) VALUES (3, {b3_literal});")

    # Retrieve
    rows = list((await session.execute(f"SELECT * FROM {table}")).iter_rows())
    assert len(rows) == 3

    r1 = next(r for r in rows if r["id"] == 1)
    r2 = next(r for r in rows if r["id"] == 2)
    r3 = next(r for r in rows if r["id"] == 3)

    b1_out = r1["b"]
    b2_out = r2["b"]
    b3_out = r3["b"]

    # Type checks
    assert isinstance(b1_out, bytes)
    assert isinstance(b2_out, bytes)
    assert isinstance(b3_out, bytes)

    # Value checks
    assert bytes(b1_out) == b1
    assert bytes(b2_out) == b2
    assert bytes(b3_out) == b3


# Verifies correct handling of NULL tuples and NULL elements inside CQL tuples
@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_tuple_with_null_elements_deserialization(session: Session, table_factory: TableFactory):
    table = await table_factory(
        """
        id int PRIMARY KEY,
        t tuple<int, text, boolean, double>
        """,
        "tuple_null_elem_table",
    )

    # Insert tuple with NULLs inside
    await session.execute(f"INSERT INTO {table} (id, t) VALUES (1, (1, NULL, true, NULL));")

    await session.execute(f"INSERT INTO {table} (id, t) VALUES (2, NULL);")

    rows = list((await session.execute(f"SELECT * FROM {table}")).iter_rows())

    r1 = next(r for r in rows if r["id"] == 1)
    r2 = next(r for r in rows if r["id"] == 2)

    # Whole tuple NULL → None
    assert r2["t"] is None

    # Tuple with NULL elements → Python tuple with None
    assert isinstance(r1["t"], tuple)
    assert r1["t"] == (1, None, True, None)


# Verifies correct handling of NULL UDT and NULL elements inside CQL UDT
@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_udt_with_null_fields_deserialization(session: Session, table_factory: TableFactory):
    # Create UDT
    await session.execute(
        """
        CREATE TYPE udt_null_test (
            a int,
            b text,
            c boolean,
            d double
        )
        """
    )

    table = await table_factory(
        """
        id int PRIMARY KEY,
        u frozen<udt_null_test>
        """,
        "udt_null_elem_table",
    )

    # Insert UDT with NULL fields
    await session.execute(f"INSERT INTO {table} (id, u) VALUES (1, {{a: 1, b: NULL, c: true, d: NULL}});")

    # Insert NULL UDT
    await session.execute(f"INSERT INTO {table} (id, u) VALUES (2, NULL);")

    rows = list((await session.execute(f"SELECT * FROM {table}")).iter_rows())

    r1 = next(r for r in rows if r["id"] == 1)
    r2 = next(r for r in rows if r["id"] == 2)

    # Whole UDT NULL → None
    assert r2["u"] is None

    # UDT with NULL fields → Python object with None fields
    assert isinstance(r1["u"], dict)
    assert r1["u"] == {
        "a": 1,
        "b": None,
        "c": True,
        "d": None,
    }
