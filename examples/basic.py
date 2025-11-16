import asyncio

from scylla.session_builder import SessionBuilder


async def main():
    builder = SessionBuilder(["172.42.0.2"], 9042)
    session = await builder.connect()
    print(
        await session.execute(
            "CREATE KEYSPACE IF NOT EXISTS example_ks WITH REPLICATION = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1}"
        )
    )
    print(await session.execute("CREATE TABLE example_ks.t (a int PRIMARY KEY, b text, c int)"))
    print(await session.execute("INSERT INTO example_ks.t (a, b, c) VALUES (1, 'sdasdad', 3)"))
    print(await session.execute("SELECT a, b, c FROM example_ks.t WHERE a = 1"))


asyncio.run(main())
