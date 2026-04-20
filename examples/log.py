import asyncio
import logging

from scylla.session_builder import SessionBuilder

logger = logging.getLogger(__name__)


async def main():
    # The logging level can be set using the standard levels:
    #   CRITICAL (50), ERROR (40), WARNING (30), INFO (20), DEBUG (10)
    #
    # To enable very verbose "TRACE" messages (more detailed than DEBUG),
    # set the level to 5:
    #   logging.basicConfig(level=5, ...)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    print(logger.getEffectiveLevel())
    print(logging.getLevelName(logger.getEffectiveLevel()))

    host = "127.0.0.2"
    port = 9042
    logger.info(f"Connecting to {host}:{port}")

    session = await SessionBuilder([host], port).connect()

    await session.execute(
        "CREATE KEYSPACE IF NOT EXISTS example_ks WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1};"
    )
    await session.execute("USE example_ks;")


asyncio.run(main())
