import asyncio
import sys

from scylla.session import Session
from scylla.statement import PreparedStatement

from common import (
    init_simple_table,
    SIMPLE_INSERT_QUERY,
    simple_table_cleanup,
    get_simple_data,
    init_with_inserts,
    SELECT_QUERY,
)

CONCURRENCY = 100


async def set_up() -> tuple[Session, int, int, PreparedStatement]:
    cnt = int(sys.argv[1])
    num_of_rows = 10

    session = await init_with_inserts(num_of_rows, SIMPLE_INSERT_QUERY, init_simple_table, get_simple_data)

    prepared = await session.prepare(SELECT_QUERY)

    return session, cnt, num_of_rows, prepared


async def clean_up(session: Session):
    await simple_table_cleanup(session)


async def select_data(
    session: Session,
    start_index: int,
    cnt: int,
    num_of_rows: int,
    prepared: PreparedStatement,
) -> None:
    index = start_index

    while index < cnt:
        res = await session.execute(prepared, None, paged=False)

        rows = await res.all()

        assert len(rows) == num_of_rows
        index += CONCURRENCY


async def main() -> None:
    session, cnt, num_of_rows, prepared = await set_up()

    tasks = [asyncio.create_task(select_data(session, i, cnt, num_of_rows, prepared)) for i in range(CONCURRENCY)]

    await asyncio.gather(*tasks)

    await clean_up(session)


if __name__ == "__main__":
    asyncio.run(main())
