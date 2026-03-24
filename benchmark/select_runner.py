import sys


from scylla.session import Session

from common import (
    init_simple_table,
    SIMPLE_INSERT_QUERY,
    simple_table_cleanup,
    SELECT_QUERY,
    init_with_inserts,
    get_simple_data,
)


async def set_up(num_of_rows: int) -> tuple[Session, int, int]:
    cnt = int(sys.argv[1])

    session = await init_with_inserts(num_of_rows, SIMPLE_INSERT_QUERY, init_simple_table, get_simple_data)

    return session, cnt, num_of_rows


async def test(session: Session, cnt: int, num_of_rows: int):
    prepared = await session.prepare(SELECT_QUERY)
    for _ in range(cnt):
        res = await session.execute(prepared, None, paged=False)
        list = await res.all()

        assert len(list) == num_of_rows


async def cleanup(session: Session):
    await simple_table_cleanup(session)


async def main(num_of_rows: int):
    session, cnt, num_of_rows = await set_up(num_of_rows)
    await test(session, cnt, num_of_rows)
    await cleanup(session)
