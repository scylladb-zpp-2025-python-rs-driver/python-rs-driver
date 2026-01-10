from typing import Callable, Awaitable, AsyncGenerator

import pytest
import pytest_asyncio
from scylla.session import Session
from scylla.session_builder import SessionBuilder


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


async def insert_rows(
    session: Session,
    table: str,
    count: int,
):
    for i in range(count):
        await session.execute(f"INSERT INTO {table} (id, x) VALUES ({i}, {i * 10});")


@pytest.mark.asyncio
@pytest.mark.requires_db
@pytest.mark.parametrize("total_rows,page_size", [(25, 10), (100, 10), (100, 1), (20, 100), (0, 10)])
async def test_execute_paged_basic_flow(session: Session, table_factory: TableFactory, total_rows: int, page_size: int):
    table = await table_factory(
        "id int PRIMARY KEY, x int",
        "paging_basic_table",
    )

    await insert_rows(session, table, total_rows)

    prepared = await session.prepare(f"SELECT * FROM {table}")
    prepared = prepared.with_page_size(page_size)

    paging_result = await session.execute_paged(prepared)

    seen_ids: list[int] = []

    number_of_pages = total_rows // page_size

    for _ in range(number_of_pages):
        assert paging_result.has_more_pages() is True

        page = list(paging_result.iter_page())
        assert len(page) == page_size

        seen_ids.extend(row["id"] for row in page)
        await paging_result.fetch_next_page()

    assert paging_result.has_more_pages() is False

    page3 = list(paging_result.iter_page())
    assert len(page3) == total_rows - number_of_pages * page_size

    seen_ids.extend(row["id"] for row in page3)

    assert sorted(seen_ids) == list(range(total_rows))


@pytest.mark.asyncio
@pytest.mark.requires_db
@pytest.mark.parametrize("total_rows,page_size", [(25, 10), (100, 10), (100, 1), (20, 100), (0, 10)])
async def test_execute_paged_basic_flow_for_unprepared_statements(
    session: Session, table_factory: TableFactory, total_rows: int, page_size: int
):
    table = await table_factory(
        "id int PRIMARY KEY, x int",
        "paging_basic_table",
    )

    await insert_rows(session, table, total_rows)

    paging_result = await session.execute_paged(f"SELECT * FROM {table}", page_size=page_size)

    seen_ids: list[int] = []

    number_of_pages = total_rows // page_size

    for _ in range(number_of_pages):
        assert paging_result.has_more_pages() is True

        page = list(paging_result.iter_page())
        assert len(page) == page_size

        seen_ids.extend(row["id"] for row in page)
        await paging_result.fetch_next_page()

    assert paging_result.has_more_pages() is False

    page3 = list(paging_result.iter_page())
    assert len(page3) == total_rows - number_of_pages * page_size

    seen_ids.extend(row["id"] for row in page3)

    assert sorted(seen_ids) == list(range(total_rows))


@pytest.mark.asyncio
@pytest.mark.requires_db
@pytest.mark.parametrize(
    "total_rows,page_size",
    [(25, 10), (100, 10), (100, 1), (20, 100), (0, 10)],
)
async def test_execute_async_paged_basic_flow(
    session: Session,
    table_factory: TableFactory,
    total_rows: int,
    page_size: int,
):
    table = await table_factory(
        "id int PRIMARY KEY, x int",
        "paging_async_basic_table",
    )

    await insert_rows(session, table, total_rows)

    prepared = await session.prepare(f"SELECT * FROM {table}")
    prepared = prepared.with_page_size(page_size)

    rows_iter = await session.execute_async_paged(prepared)

    seen_ids: list[int] = []

    async for row in rows_iter:
        assert isinstance(row, dict)
        assert "id" in row

        seen_ids.append(row["id"])

    assert sorted(seen_ids) == list(range(total_rows))


@pytest.mark.asyncio
@pytest.mark.requires_db
@pytest.mark.parametrize(
    "total_rows,page_size",
    [(25, 10), (100, 10), (100, 1), (20, 100), (0, 10)],
)
async def test_execute_async_paged_for_string_query(
    session: Session,
    table_factory: TableFactory,
    total_rows: int,
    page_size: int,
):
    table = await table_factory(
        "id int PRIMARY KEY, x int",
        "paging_async_basic_table",
    )

    await insert_rows(session, table, total_rows)

    rows_iter = await session.execute_async_paged(f"SELECT * FROM {table}", page_size=page_size)

    seen_ids: list[int] = []

    async for row in rows_iter:
        assert isinstance(row, dict)
        assert "id" in row

        seen_ids.append(row["id"])

    assert sorted(seen_ids) == list(range(total_rows))
