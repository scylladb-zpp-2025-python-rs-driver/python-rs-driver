from typing import Callable, Awaitable, AsyncGenerator, Any, cast
import warnings

import pytest
import pytest_asyncio
import asyncio
import gc

from scylla.statement import Statement
from scylla.session import Session
from scylla.session_builder import SessionBuilder
from scylla.results import PagingState


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

    paging_result = await session.execute(prepared)

    seen_ids: list[int] = []

    number_of_pages = total_rows // page_size

    for _ in range(number_of_pages):
        assert paging_result.has_more_pages() is True

        page = list(paging_result.iter_current_page())
        assert len(page) == page_size

        seen_ids.extend(row["id"] for row in page)
        next_page = await paging_result.fetch_next_page()
        assert next_page is not None
        paging_result = next_page

    assert paging_result.has_more_pages() is False

    page3 = list(paging_result.iter_current_page())
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

    statement = Statement(f"SELECT * FROM {table}")
    statement = statement.with_page_size(page_size)

    paging_result = await session.execute(statement)

    seen_ids: list[int] = []

    number_of_pages = total_rows // page_size

    for _ in range(number_of_pages):
        assert paging_result.has_more_pages() is True

        page = list(paging_result.iter_current_page())
        assert len(page) == page_size

        seen_ids.extend(row["id"] for row in page)
        next_page = await paging_result.fetch_next_page()
        assert next_page is not None
        paging_result = next_page

    assert paging_result.has_more_pages() is False

    page3 = list(paging_result.iter_current_page())
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

    rows_iter = await session.execute(prepared)

    seen_ids: list[Any] = []

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

    statement = Statement(f"SELECT * FROM {table}")
    statement = statement.with_page_size(page_size)

    rows_iter = await session.execute(statement)

    seen_ids: list[Any] = []

    async for row in rows_iter:
        assert isinstance(row, dict)
        assert "id" in row

        seen_ids.append(row["id"])

    assert sorted(seen_ids) == list(range(total_rows))


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_paging_state_resume(
    session: Session,
    table_factory: TableFactory,
):
    table = await table_factory(
        "id int PRIMARY KEY, x int",
        "paging_resume_table",
    )

    await insert_rows(session, table, 20)

    prepared = await session.prepare(f"SELECT * FROM {table}")
    prepared = prepared.with_page_size(10)

    result = await session.execute(prepared)

    seen_ids: list[int] = []

    while True:
        page = list(result.iter_current_page())
        seen_ids.extend(row["id"] for row in page)

        state = result.paging_state()
        if state is None:
            break

        result = await session.execute(
            prepared,
            paging_state=state,
        )

    assert sorted(seen_ids) == list(range(20))


@pytest.mark.asyncio
@pytest.mark.requires_db
@pytest.mark.parametrize("total_rows,page_size", [(0, 10), (5, 2), (25, 10), (1000, 10)])
async def test_paging_all_returns_all_rows(
    session: Session,
    table_factory: TableFactory,
    total_rows: int,
    page_size: int,
):
    table = await table_factory(
        "id int PRIMARY KEY, x int",
        "paging_all_table",
    )

    await insert_rows(session, table, total_rows)

    prepared = await session.prepare(f"SELECT * FROM {table}")
    prepared = prepared.with_page_size(page_size)

    result = await session.execute(prepared)

    rows = await result.all()

    assert isinstance(rows, list)
    assert len(rows) == total_rows

    ids = [row["id"] for row in rows]
    assert sorted(ids) == list(range(total_rows))


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_paging_one_returns_none_for_empty_result(
    session: Session,
    table_factory: TableFactory,
):
    table = await table_factory(
        "id int PRIMARY KEY, x int",
        "paging_one_empty_table",
    )

    prepared = await session.prepare(f"SELECT * FROM {table}")
    result = await session.execute(prepared)

    row = await result.single_row()

    assert row is None


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_paging_one_returns_first_row(
    session: Session,
    table_factory: TableFactory,
):
    table = await table_factory(
        "id int PRIMARY KEY, x int",
        "paging_one_table",
    )

    await insert_rows(session, table, 1)

    prepared = await session.prepare(f"SELECT * FROM {table}")
    prepared = prepared.with_page_size(2)

    result = await session.execute(prepared)

    row = await result.single_row()

    assert row is not None
    assert row["id"] == 0


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_first_for_non_rows_result_returns_none(
    session: Session,
    table_factory: TableFactory,
):
    table = await table_factory(
        "id int PRIMARY KEY, x int",
        "paging_one_table",
    )
    result = await session.execute(f"INSERT INTO {table} (id, x) VALUES (1000, 42)")

    row_first = await result.single_row()
    row_all = await result.all()

    assert row_first is None
    assert row_all == []


def test_paging_state_new_is_start_state():
    state = PagingState()

    assert state.as_bytes() is None


def test_paging_state_from_bytes_roundtrip():
    raw = b"\x01\x02\x03\x04"

    state = PagingState.from_bytes(raw)

    assert state.as_bytes() == raw


def test_paging_state_start_and_from_bytes_are_not_equal():
    start_state = PagingState()
    resumed_state = PagingState.from_bytes(b"\x01\x02\x03")

    assert start_state != resumed_state


def test_paging_state_equal_for_same_raw_bytes():
    state1 = PagingState.from_bytes(b"\x01\x02\x03")
    state2 = PagingState.from_bytes(b"\x01\x02\x03")

    assert state1 == state2


def test_paging_state_not_equal_for_different_raw_bytes():
    state1 = PagingState.from_bytes(b"\x01\x02\x03")
    state2 = PagingState.from_bytes(b"\x04\x05\x06")

    assert state1 != state2


def test_paging_state_from_empty_bytes_is_not_start_state():
    state = PagingState.from_bytes(b"")

    assert state.as_bytes() == b""
    assert state != PagingState()


WARNING_TEXT = "Query result dropped before being fully consumed"


async def _flush_drops() -> None:
    gc.collect()
    await asyncio.sleep(0)


def _assert_warning_raised(warning_list: list[warnings.WarningMessage]) -> None:
    """Assert that at least one RuntimeWarning with the expected message was raised."""
    assert len(warning_list) > 0, "Expected a RuntimeWarning to be raised, but none were caught"
    assert any(WARNING_TEXT in str(w.message) for w in warning_list), (
        f"Expected warning containing '{WARNING_TEXT}' but got: {[str(w.message) for w in warning_list]}"
    )


def _assert_no_warnings(warning_list: list[warnings.WarningMessage]) -> None:
    """Assert that no RuntimeWarning with the expected message was raised."""
    matching_warnings: list[warnings.WarningMessage] = [w for w in warning_list if WARNING_TEXT in str(w.message)]
    assert len(matching_warnings) == 0, f"Expected no warnings but got: {[str(w.message) for w in matching_warnings]}"


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_no_warning_for_non_rows_result_if_user_does_not_check_it(
    session: Session,
    table_factory: TableFactory,
):
    table = await table_factory(
        "id int PRIMARY KEY, x int",
        "warning_non_rows_insert_table",
    )

    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")

        result: Any = await session.execute(f"INSERT INTO {table} (id, x) VALUES (1000, 42)")

        del result
        await _flush_drops()

        _assert_no_warnings(w)


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_warning_for_unfinished_async_iteration(
    session: Session,
    table_factory: TableFactory,
):
    table = await table_factory(
        "id int PRIMARY KEY, x int",
        "warning_async_unfinished_table",
    )
    await insert_rows(session, table, 10)

    prepared = await session.prepare(f"SELECT * FROM {table}")
    prepared = prepared.with_page_size(3)

    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")

        result = await session.execute(prepared)
        async_iter = result.__aiter__()

        row = await async_iter.__anext__()
        assert row is not None

        del async_iter
        del result
        await _flush_drops()

        _assert_warning_raised(w)


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_no_warning_when_one_async_iterator_exhausts_even_if_another_does_not(
    session: Session,
    table_factory: TableFactory,
):
    table = await table_factory(
        "id int PRIMARY KEY, x int",
        "warning_two_async_iters_table",
    )
    await insert_rows(session, table, 10)

    prepared = await session.prepare(f"SELECT * FROM {table}")
    prepared = prepared.with_page_size(3)

    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")

        result = await session.execute(prepared)

        iter1 = result.__aiter__()
        iter2 = result.__aiter__()

        first_row = await iter1.__anext__()
        assert first_row is not None

        seen: list[int] = []
        async for row in iter2:
            seen.append(cast(int, row["id"]))

        assert sorted(seen) == list(range(10))

        del iter1
        del iter2
        del result
        await _flush_drops()

        _assert_no_warnings(w)


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_warning_for_empty_result_if_user_does_not_check_it(
    session: Session,
    table_factory: TableFactory,
):
    table = await table_factory(
        "id int PRIMARY KEY, x int",
        "warning_empty_unchecked_table",
    )

    prepared = await session.prepare(f"SELECT * FROM {table}")

    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")

        result = await session.execute(prepared)

        del result
        await _flush_drops()

        _assert_warning_raised(w)


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_no_warning_for_empty_result_when_single_row_is_used(
    session: Session,
    table_factory: TableFactory,
):
    table = await table_factory(
        "id int PRIMARY KEY, x int",
        "warning_empty_single_row_table",
    )

    prepared = await session.prepare(f"SELECT * FROM {table}")

    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")

        result = await session.execute(prepared)

        row = await result.single_row()
        assert row is None

        del result
        await _flush_drops()

        _assert_no_warnings(w)


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_no_warning_for_single_row_result_when_single_row_is_used(
    session: Session,
    table_factory: TableFactory,
):
    table = await table_factory(
        "id int PRIMARY KEY, x int",
        "warning_single_row_single_row_table",
    )
    await insert_rows(session, table, 1)

    prepared = await session.prepare(f"SELECT * FROM {table}")
    prepared = prepared.with_page_size(10)

    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")

        result = await session.execute(prepared)

        row = await result.single_row()
        assert row is not None
        assert row["id"] == 0

        del result
        await _flush_drops()

        _assert_no_warnings(w)


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_no_warning_when_all_is_used(
    session: Session,
    table_factory: TableFactory,
):
    table = await table_factory(
        "id int PRIMARY KEY, x int",
        "warning_all_table",
    )
    await insert_rows(session, table, 10)

    prepared = await session.prepare(f"SELECT * FROM {table}")
    prepared = prepared.with_page_size(3)

    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")

        result = await session.execute(prepared)

        rows = await result.all()
        assert len(rows) == 10

        del result
        await _flush_drops()

        _assert_no_warnings(w)


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_warning_for_manual_paging_without_exhausting(
    session: Session,
    table_factory: TableFactory,
):
    table = await table_factory(
        "id int PRIMARY KEY, x int",
        "warning_manual_paging_unfinished_table",
    )
    await insert_rows(session, table, 10)

    prepared = await session.prepare(f"SELECT * FROM {table}")
    prepared = prepared.with_page_size(3)

    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")

        result = await session.execute(prepared)

        page = list(result.iter_current_page())
        assert len(page) == 3

        del result
        await _flush_drops()

        _assert_warning_raised(w)


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_no_warning_when_manual_paging_and_async_iteration_share_result_and_async_exhausts(
    session: Session,
    table_factory: TableFactory,
):
    table = await table_factory(
        "id int PRIMARY KEY, x int",
        "warning_manual_plus_async_table",
    )
    await insert_rows(session, table, 10)

    prepared = await session.prepare(f"SELECT * FROM {table}")
    prepared = prepared.with_page_size(3)

    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")

        result: Any = await session.execute(prepared)

        first_page = list(result.iter_current_page())
        assert len(first_page) == 3

        seen: list[int] = []
        async for row in result:
            seen.append(cast(int, row["id"]))

        assert sorted(seen) == list(range(10))

        del result
        await _flush_drops()

        _assert_no_warnings(w)


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_no_warning_after_manual_fetch_next_page_chain_exhausts_result(
    session: Session,
    table_factory: TableFactory,
):
    table = await table_factory(
        "id int PRIMARY KEY, x int",
        "warning_manual_chain_exhausted_table",
    )
    await insert_rows(session, table, 10)

    prepared = await session.prepare(f"SELECT * FROM {table}")
    prepared = prepared.with_page_size(3)

    with warnings.catch_warnings(record=True) as w:
        warnings.simplefilter("always")

        result = await session.execute(prepared)

        seen: list[int] = []
        while True:
            seen.extend(cast(int, row["id"]) for row in result.iter_current_page())
            next_page: Any = await result.fetch_next_page()
            if next_page is None:
                break
            result = next_page

        assert sorted(seen) == list(range(10))

        del result
        await _flush_drops()

        _assert_no_warnings(w)
