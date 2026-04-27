import sys

import asyncio

from scylla.session import Session

from common import (
    COMPLEX_INSERT_QUERY,
    simple_table_cleanup,
    SELECT_QUERY,
    init_with_inserts,
    init_complex_table,
    get_complex_data,
)


async def set_up() -> tuple[Session, int, int]:
    cnt = int(sys.argv[1])
    # num_of_rows = int(sys.argv[2])
    # For now in benchmarks there is support only for one argument
    num_of_rows = cnt

    session = await init_with_inserts(num_of_rows, COMPLEX_INSERT_QUERY, init_complex_table, get_complex_data)

    return session, cnt, num_of_rows


async def test(session: Session, cnt: int, num_of_rows: int):
    prepared = await session.prepare(SELECT_QUERY)
    for _ in range(cnt):
        res = await session.execute(prepared, None, paged=False)
        list = await res.all()

        assert len(list) == num_of_rows


async def cleanup(session: Session):
    await simple_table_cleanup(session)


async def main():
    session, cnt, num_of_rows = await set_up()
    await test(session, cnt, num_of_rows)
    await cleanup(session)


if __name__ == "__main__":
    asyncio.run(main())
