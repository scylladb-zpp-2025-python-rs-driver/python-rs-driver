"""
paging.py

Example showcasing multiple ways to consume ScyllaDB query results with the new Python driver API.

This file demonstrates:
  1) Simple async iteration over all rows (auto-paging under the hood)
  2) Manual paging: iter_current_page() + fetch_next_page()
  3) Manual paging with explicit PagingState resume
  4) Convenience helpers: single_row() and all()
  5) Custom row shaping via RowFactory

"""

import asyncio
import os
from typing import Any, Dict

from scylla.session_builder import SessionBuilder
from scylla.session import Session
from scylla.statement import Statement
from scylla.results import RowFactory, ColumnIterator


# ----------------------------
# DB setup helpers
# ----------------------------
async def setup_schema(session: Session) -> None:
    # Create keyspace & table.
    await session.execute(
        """
        CREATE KEYSPACE IF NOT EXISTS examples_ks
        WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1};
        """
    )
    await session.execute("USE examples_ks")

    await session.execute(
        """
        CREATE TABLE IF NOT EXISTS select_paging (
                                                     a int,
                                                     b int,
                                                     c text,
                                                     PRIMARY KEY (a, b)
            );
        """
    )

    # Insert a small deterministic dataset.
    # (Re-inserting is fine: primary key makes rows idempotent for the same keys.)
    for i in range(16):
        await session.execute(
            "INSERT INTO select_paging (a, b, c) VALUES (?, ?, 'abc')",
            values=(i, 2 * i),
        )


# ----------------------------
# 1) Easiest: async for (auto-paging)
# ----------------------------
async def example_async_for(session: Session) -> None:
    print("\n=== 1) Async iteration over all rows (auto-paging) ===")

    # Unprepared string query (supports str | Statement | PreparedStatement).
    result = await session.execute("SELECT a, b, c FROM select_paging")

    async for row in result:
        # Default row representation: dict[str, CqlValue]
        print(f"row={row}")


# ----------------------------
# 2) Manual paging loop: iter_current_page() + fetch_next_page()
# ----------------------------
async def example_manual_paging_unprepared(session: Session) -> None:
    print("\n=== 2) Manual paging (unprepared Statement) ===")

    stmt = Statement("SELECT a, b, c FROM select_paging").with_page_size(6)
    result = await session.execute(stmt)

    page_no = 1
    while True:
        # Consume only the *current* page:
        page_rows = list(result.iter_current_page())
        print(f"page {page_no}: {len(page_rows)} rows -> {page_rows}")

        if not result.has_more_pages():
            break

        # Fetch the next page (returns a new RequestResult):
        next_result = await result.fetch_next_page()
        if next_result is None:
            break
        result = next_result
        page_no += 1


async def example_manual_paging_prepared(session: Session) -> None:
    print("\n=== 3) Manual paging (prepared statement) ===")

    prepared = await session.prepare("SELECT a, b, c FROM select_paging")
    # Setting page size on the prepared statement applies to all executions of it
    prepared = prepared.with_page_size(7)

    result = await session.execute(prepared)

    page_no = 1
    while True:
        # Consume only the *current* page of size 7 except maybe the last one:
        page_rows = list(result.iter_current_page())
        print(f"page {page_no}: {len(page_rows)} rows")

        if not result.has_more_pages():
            break

        # Fetch the next page (returns a new RequestResult):
        next_result = await result.fetch_next_page()
        if next_result is None:
            break
        result = next_result
        page_no += 1


# ----------------------------
# 3) PagingState: resume later
# ----------------------------
async def example_paging_state_resume(session: Session) -> None:
    print("\n=== 4) PagingState resume ===")

    prepared = await session.prepare("SELECT a, b, c FROM select_paging")
    prepared = prepared.with_page_size(5)

    # Fetch first page
    r1 = await session.execute(prepared)
    first_page = list(r1.iter_current_page())
    state = r1.paging_state()

    print(f"first_page size={len(first_page)}")

    if state is None:
        print("No paging state (result fit in one page). Nothing to resume.")
        return

    # Resume: new execute call with the returned paging_state starts from "after the first page"
    r2 = await session.execute(prepared, paging_state=state)
    second_page = list(r2.iter_current_page())

    print(f"resumed_page size={len(second_page)}")

    # Quick sanity check: pages should not overlap
    ids_first = {row["a"] for row in first_page}
    ids_second = {row["a"] for row in second_page}
    assert ids_first.isdisjoint(ids_second), "Resumed page overlapped the first page!"


# ----------------------------
# 4) Convenience helpers: single_row() and all()
# ----------------------------
async def example_single_row_and_all(session: Session) -> None:
    print("\n=== 5) Convenience helpers: single_row() and all() ===")

    prepared = await session.prepare("SELECT a, b, c FROM select_paging")
    prepared = prepared.with_page_size(4)

    result = await session.execute(prepared)

    # single_row(): returns one row or None (does not force consuming the full result set)
    one = await result.single_row()
    print(f"single_row() -> {one}")

    # all(): eagerly fetches all remaining pages and materializes into a list
    rows = await result.all()
    print(f"all() -> {len(rows)} rows")


# ----------------------------
# A custom RowFactory example
# ----------------------------
class TupleRowFactory(RowFactory):
    """
    Example: convert each row into a plain tuple of values.
    """

    def build(self, column_iterator: ColumnIterator) -> Any:
        return tuple(col.value for col in column_iterator)


class UppercaseKeysDictFactory(RowFactory):
    """
    Example: dict row, but keys uppercased.
    """

    def build(self, column_iterator: ColumnIterator) -> Dict[str, Any]:
        return {col.column_name.upper(): col.value for col in column_iterator}


# ----------------------------
# 5) Custom row shapes (RowFactory)
# ----------------------------
async def example_custom_row_factory(session: Session) -> None:
    print("\n=== 5) Custom row factories ===")

    stmt = Statement("SELECT a, b, c FROM select_paging").with_page_size(5)
    result = await session.execute(stmt, factory=UppercaseKeysDictFactory())

    page = list(result.iter_current_page())
    print(f"set_factory(UppercaseKeysDictFactory()); first row -> {page[0]}")

    # And async iteration will now yield that shape:
    result = await session.execute(stmt, factory=TupleRowFactory())

    seen: list[Any] = []
    async for row in result:
        seen.append(row)
        if len(seen) >= 3:
            break
    print(f"async for with TupleRowFactory (first 3) -> {seen}")


async def main() -> None:
    uri = os.getenv("SCYLLA_URI", "127.0.0.2:9042")
    host, port_str = uri.split(":")
    port = int(port_str)

    print(f"Connecting to {host}:{port} ...")
    session = await SessionBuilder([host], port).connect()

    await setup_schema(session)

    await example_async_for(session)
    await example_manual_paging_unprepared(session)
    await example_manual_paging_prepared(session)
    await example_paging_state_resume(session)
    await example_single_row_and_all(session)
    await example_custom_row_factory(session)

    # Cleanup
    await session.execute("DROP TABLE IF EXISTS examples_ks.select_paging")
    print("\nTable dropped.")

    print("\nOk.")


if __name__ == "__main__":
    asyncio.run(main())
