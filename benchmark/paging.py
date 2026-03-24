import sys

import asyncio
from scylla.statement import PreparedStatement

from scylla.session import Session

from common import (
    init_simple_table,
    SIMPLE_INSERT_QUERY,
    simple_table_cleanup,
    SELECT_QUERY,
    init_with_inserts,
    get_simple_data,
)


async def set_up() -> tuple[Session, PreparedStatement, int, int]:
    cnt = int(sys.argv[1])
    # num_of_rows = int(sys.argv[2])
    num_of_rows = 50

    session = await init_with_inserts(num_of_rows, SIMPLE_INSERT_QUERY, init_simple_table, get_simple_data)

    prepared = await session.prepare(SELECT_QUERY)
    prepared = prepared.with_page_size(1)

    return session, prepared, cnt, num_of_rows


async def test(session: Session, prepared: PreparedStatement, cnt: int, num_of_rows: int):
    for _ in range(cnt):
        count = 0
        result = await session.execute(prepared)

        while True:
            for row in result.iter_current_page():
                count += row["val"]

            next_result = await result.fetch_next_page()
            if next_result is None:
                break

            result = next_result

        assert count == num_of_rows * 100


async def cleanup(session: Session):
    await simple_table_cleanup(session)


async def main():
    session, prepared, cnt, num_of_rows = await set_up()
    await test(session, prepared, cnt, num_of_rows)
    await cleanup(session)


if __name__ == "__main__":
    asyncio.run(main())
