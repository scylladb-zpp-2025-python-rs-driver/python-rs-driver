import sys
import asyncio
import uuid

from common import (
    init_simple_table,
    SIMPLE_INSERT_QUERY,
    check_row_cnt,
    simple_table_cleanup,
)
from scylla.batch import Batch, BatchType
from scylla.session import Session
from scylla.statement import PreparedStatement, Statement

STEP = 3971


async def set_up() -> tuple[Session, PreparedStatement, int]:
    cnt = int(sys.argv[1])

    session = await init_simple_table()
    prepared = await session.prepare(Statement(SIMPLE_INSERT_QUERY))

    return session, prepared, cnt


async def test(
    session: Session,
    prepared: PreparedStatement,
    cnt: int,
) -> None:
    for start in range(0, cnt, STEP):
        batch_len = min(cnt - start, STEP)

        batch = Batch(BatchType.Logged)
        for _ in range(batch_len):
            batch.add(prepared, (uuid.uuid4(), 1))

        await session.batch(batch)


async def cleanup(session: Session, cnt: int) -> None:
    await check_row_cnt(session, cnt)
    await simple_table_cleanup(session)


async def main() -> None:
    session, prepared, cnt = await set_up()
    await test(session, prepared, cnt)
    await cleanup(session, cnt)


if __name__ == "__main__":
    asyncio.run(main())
