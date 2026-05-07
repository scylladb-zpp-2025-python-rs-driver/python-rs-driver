import sys

import asyncio

from scylla.session import Session
from scylla.statement import PreparedStatement
from scylla.statement import Statement

from common import (
    connect,
    SIMPLE_INSERT_QUERY,
    get_simple_data,
)


async def test(session: Session, prepared: PreparedStatement, cnt: int):
    for _ in range(cnt):
        await session.execute(prepared, get_simple_data(), paged=False)


async def main():
    cnt = int(sys.argv[1])
    session = await connect()
    statement = Statement(SIMPLE_INSERT_QUERY)
    prepared = await session.prepare(statement)

    await test(session, prepared, cnt)


if __name__ == "__main__":
    asyncio.run(main())
