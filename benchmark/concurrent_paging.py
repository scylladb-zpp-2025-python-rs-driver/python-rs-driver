import asyncio
import sys

from scylla.session import Session
from scylla.statement import PreparedStatement

from common import (
    init_simple_table,
    SIMPLE_INSERT_QUERY,
    SELECT_QUERY,
    simple_table_cleanup,
    init_with_inserts,
    get_simple_data,
)

CONCURRENCY_LEVEL = 100


async def set_up() -> tuple[Session, PreparedStatement, int, int]:
    cnt = int(sys.argv[1])
    num_of_rows = 50

    session = await init_with_inserts(
        num_of_rows,
        SIMPLE_INSERT_QUERY,
        init_simple_table,
        get_simple_data,
    )

    prepared = await session.prepare(SELECT_QUERY)
    prepared = prepared.with_page_size(1)

    return session, prepared, cnt, num_of_rows


async def paging_worker(
    session: Session,
    prepared: PreparedStatement,
    cnt: int,
    start_index: int,
    expected_sum: int,
) -> None:
    index = start_index

    while index < cnt:
        result = await session.execute(prepared)

        total = 0
        while result is not None:
            for row in result.iter_current_page():
                total += row["val"]

            result = await result.fetch_next_page()

        assert total == expected_sum
        index += CONCURRENCY_LEVEL


async def cleanup(session: Session) -> None:
    await simple_table_cleanup(session)


async def main() -> None:
    session, prepared, cnt, num_of_rows = await set_up()

    expected_sum = num_of_rows * 100

    tasks = [
        asyncio.create_task(paging_worker(session, prepared, cnt, i, expected_sum)) for i in range(CONCURRENCY_LEVEL)
    ]

    await asyncio.gather(*tasks)

    await cleanup(session)


if __name__ == "__main__":
    asyncio.run(main())
