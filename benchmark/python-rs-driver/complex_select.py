import sys
import asyncio

from scylla.session import Session

from common import (
    connect,
    COMPLEX_SELECT_QUERY,
)


async def test(session: Session, cnt: int, num_of_rows: int):
    prepared = await session.prepare(COMPLEX_SELECT_QUERY)
    for _ in range(cnt):
        res = await session.execute(prepared, None, paged=False)
        rows = await res.all()

        assert len(rows) == num_of_rows


async def main():
    num_of_rows = int(sys.argv[1])
    cnt = int(sys.argv[2])
    session = await connect()
    await test(session, cnt, num_of_rows)


if __name__ == "__main__":
    asyncio.run(main())
