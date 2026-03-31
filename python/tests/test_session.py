import pytest
import pytest_asyncio

import uuid

from scylla.session import Session
from scylla.session_builder import SessionBuilder


async def set_up() -> Session:
    builder = SessionBuilder(["127.0.0.2"], 9042)
    session = await builder.connect()

    await session.execute("""
            CREATE KEYSPACE IF NOT EXISTS testks
            WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': 1};
        """)

    await session.execute("USE testks")

    return session


@pytest_asyncio.fixture(scope="module")
async def session():
    session = await set_up()
    yield session
    await session.execute("DROP KEYSPACE testks")


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_check_schema_agreement_returns_schema_version_or_none(session: Session):
    schema_version = await session.check_schema_agreement()

    assert schema_version is None or isinstance(schema_version, uuid.UUID)


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_await_schema_agreement_returns_schema_version(session: Session):
    schema_version = await session.await_schema_agreement()

    assert isinstance(schema_version, uuid.UUID)
    assert schema_version
