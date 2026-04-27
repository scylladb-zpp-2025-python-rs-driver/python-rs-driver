import asyncio
import sys

from scylla.session import Session
from scylla.statement import PreparedStatement

from common import (
    init_with_inserts,
    SELECT_QUERY,
    COMPLEX_INSERT_QUERY,
    init_complex_table,
    get_complex_data,
    complex_table_cleanup,
)

CONCURRENCY = 100


async def set_up() -> tuple[Session, int, int, PreparedStatement]:
    cnt = int(sys.argv[1])
    # num_of_rows = int(sys.argv[2])
    num_of_rows = 50

    session = await init_with_inserts(num_of_rows, COMPLEX_INSERT_QUERY, init_complex_table, get_complex_data)

    prepared = await session.prepare(SELECT_QUERY)

    return session, cnt, num_of_rows, prepared


async def clean_up(session: Session):
    await complex_table_cleanup(session)


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

        list = await res.all()

        assert len(list) == num_of_rows
        index += CONCURRENCY


async def main() -> None:
    session, cnt, num_of_rows, prepared = await set_up()

    tasks = [asyncio.create_task(select_data(session, i, cnt, num_of_rows, prepared)) for i in range(CONCURRENCY)]

    await asyncio.gather(*tasks)

    await clean_up(session)


if __name__ == "__main__":
    asyncio.run(main())
