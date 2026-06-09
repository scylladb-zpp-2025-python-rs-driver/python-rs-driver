import asyncio
import os
from typing import Any

from cassandra.cluster import Cluster, ResponseFuture, Session  # pyright: ignore[reportMissingTypeStubs]

# Force non-paged queries by setting a large page size
PAGE_SIZE = 1_000_000


async def connect() -> Session:
    uri = os.getenv("SCYLLA_URI", "127.0.0.2:9042")
    if ":" in uri:
        host, port_str = uri.rsplit(":", 1)
        port = int(port_str)
    else:
        host = uri
        port = 9042
    cluster = Cluster([host], port=port)
    session = cluster.connect()  # pyright: ignore[reportUnknownMemberType]
    return session


def to_asyncio(response_future: ResponseFuture) -> asyncio.Future[Any]:
    """
    Create an awaitable asyncio future from a cassandra response future.

    This merges cassandra and asyncio API by adding
    thread-safe asyncio compatible callbacks to the cassandra response future.
    """
    loop = asyncio.get_running_loop()
    fut = loop.create_future()

    def on_success(result: Any):
        loop.call_soon_threadsafe(fut.set_result, result)

    def on_error(exc: Any):
        loop.call_soon_threadsafe(fut.set_exception, exc)

    response_future.add_callbacks(on_success, on_error)  # pyright: ignore[reportUnknownMemberType]

    return fut
