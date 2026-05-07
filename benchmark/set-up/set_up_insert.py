import asyncio

from common import (
    init_simple_table,
)


async def main():
    await init_simple_table()


if __name__ == "__main__":
    asyncio.run(main())
