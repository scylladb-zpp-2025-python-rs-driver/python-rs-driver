import sys
import asyncio

from common import (
    init_simple_table,
    SIMPLE_INSERT_QUERY,
    init_with_inserts,
    get_simple_data,
)


async def main():
    num_of_rows = int(sys.argv[1])
    await init_with_inserts(num_of_rows, SIMPLE_INSERT_QUERY, init_simple_table, get_simple_data)


if __name__ == "__main__":
    asyncio.run(main())
