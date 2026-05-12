import logging

import pytest
from _pytest.logging import LogCaptureFixture
from scylla.session_builder import SessionBuilder


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_rust_logs_forwarded(caplog: LogCaptureFixture):
    logging.basicConfig(level=logging.INFO)
    caplog.set_level(logging.INFO)

    session = await SessionBuilder().contact_points([("127.0.0.2", 9042)]).connect()

    # Setting replication factor to 1 generates the following warning log:
    # Response from the database contains a warning warning="Using
    # Replication Factor replication_factor=1 lower than the
    # minimum_replication_factor_warn_threshold=3 is not recommended."
    await session.execute(
        "CREATE KEYSPACE IF NOT EXISTS example_ks WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1};"
    )
    await session.execute("USE example_ks;")

    # Assert that the warning log was captured
    assert any("Response from the database contains a warning" in rec.getMessage() for rec in caplog.records)
