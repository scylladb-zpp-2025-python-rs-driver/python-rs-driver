import asyncio
import sys

from scylla.session import Session
from scylla.statement import PreparedStatement, Statement

from common import init_complex_table, check_row_cnt, COMPLEX_INSERT_QUERY, complex_table_cleanup, get_complex_data

CONCURRENCY = 100


async def set_up() -> tuple[Session, PreparedStatement, int]:
    cnt = int(sys.argv[1])

    session = await init_complex_table()

    statement = Statement(COMPLEX_INSERT_QUERY)

    prepared = await session.prepare(statement)
    return session, prepared, cnt


async def clean_up(session: Session, cnt: int):
    await check_row_cnt(session, cnt)
    await complex_table_cleanup(session)


async def insert_data(
    session: Session,
    start_index: int,
    cnt: int,
    prepared: PreparedStatement,
) -> None:
    index = start_index

    while index < cnt:
        await session.execute(prepared, get_complex_data(), paged=False)
        index += CONCURRENCY


async def main() -> None:
    session, prepared, cnt = await set_up()

    tasks = [asyncio.create_task(insert_data(session, i, cnt, prepared)) for i in range(CONCURRENCY)]

    await asyncio.gather(*tasks)

    await clean_up(session, cnt)


if __name__ == "__main__":
    asyncio.run(main())
