import logging

import pytest
from _pytest.logging import LogCaptureFixture
from scylla.session_builder import SessionBuilder


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_rust_logs_forwarded(caplog: LogCaptureFixture):
    logging.basicConfig(level=logging.INFO)
    caplog.set_level(logging.INFO)

    host = "127.0.0.2"
    port = 9042
    session = await SessionBuilder([host], port).connect()

    await session.execute(
        "CREATE KEYSPACE IF NOT EXISTS example_ks WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1};"
    )
    await session.execute("USE example_ks;")

    assert any("Response from the database contains a warning" in rec.getMessage() for rec in caplog.records)
