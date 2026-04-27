import sys

import asyncio

from common import init_complex_table, COMPLEX_INSERT_QUERY, get_complex_data
from scylla.session import Session
from scylla.statement import PreparedStatement

from scylla.statement import Statement

from common import check_row_cnt, simple_table_cleanup


async def set_up() -> tuple[Session, PreparedStatement, int]:
    cnt = int(sys.argv[1])

    session = await init_complex_table()

    statement = Statement(COMPLEX_INSERT_QUERY)

    prepared = await session.prepare(statement)
    return session, prepared, cnt


async def test(session: Session, prepared: PreparedStatement, cnt: int):
    for _ in range(cnt):
        await session.execute(prepared, get_complex_data(), paged=False)


async def cleanup(session: Session, cnt: int):
    await check_row_cnt(session, cnt)

    await simple_table_cleanup(session)


async def main():
    session, prepared, cnt = await set_up()
    await test(session, prepared, cnt)
    await cleanup(session, cnt)


if __name__ == "__main__":
    asyncio.run(main())
