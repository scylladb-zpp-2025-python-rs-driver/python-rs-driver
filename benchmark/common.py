from typing import Callable, Awaitable, Any

from datetime import datetime, time, date, timezone

import ipaddress
import uuid
from dateutil.relativedelta import relativedelta
from scylla.statement import Statement

from scylla.session import Session

from scylla.session_builder import SessionBuilder
import os


SIMPLE_INSERT_QUERY = "INSERT INTO benchmarks.basic (id, val) VALUES (?, ?)"
COMPLEX_INSERT_QUERY = "INSERT INTO benchmarks.basic (id, val, tuuid, ip, date, time, tuple, udt, set1, duration) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
SELECT_COUNT = "SELECT COUNT(1) FROM benchmarks.basic USING TIMEOUT 120s;"
SELECT_QUERY = "SELECT * FROM benchmarks.basic"


async def init_common(schema: str) -> Session:
    uri = os.getenv("SCYLLA_URI", "127.0.0.2:9042")
    host, port = uri.split(":")
    builder = SessionBuilder([host], int(port))
    session = await builder.connect()

    await session.execute("""
       CREATE KEYSPACE IF NOT EXISTS benchmarks WITH replication = {'class': 'NetworkTopologyStrategy', 'replication_factor': '1' }
    """)

    await session.execute("USE benchmarks")
    await session.execute("DROP TABLE IF EXISTS benchmarks.basic")
    await session.execute(schema)
    return session


async def init_simple_table() -> Session:
    return await init_common("CREATE TABLE benchmarks.basic (id uuid, val int, PRIMARY KEY(id))")


async def init_complex_table() -> Session:
    session = await init_common("CREATE TYPE IF NOT EXISTS benchmarks.udt1 (field1 text, field2 int)")
    await session.execute(
        "CREATE TABLE benchmarks.basic (id uuid, val int, tuuid timeuuid, ip inet, date date, time time, tuple frozen<tuple<text, int>>, udt frozen<udt1>, set1 set<int>, duration duration, PRIMARY KEY(id))"
    )
    return session


async def init_with_inserts(
    num_of_rows: int,
    query: str,
    init: Callable[[], Awaitable[Session]],
    get_data: Callable[[], Any],
) -> Session:
    session = await init()

    statement = Statement(query)
    prepared = await session.prepare(statement)

    for _ in range(num_of_rows):
        await session.execute(prepared, get_data(), paged=False)

    return session


async def simple_table_cleanup(session: Session):
    await session.execute("DROP TABLE IF EXISTS benchmarks.basic")
    await session.execute("DROP TYPE IF EXISTS benchmarks.udt1")


async def complex_table_cleanup(session: Session):
    await simple_table_cleanup(session)


async def check_row_cnt(session: Session, cnt: int):
    res = await session.execute(SELECT_COUNT, None, paged=False)

    count = await res.first_row()
    assert count is not None
    assert count["count"] == cnt


def get_simple_data() -> tuple[uuid.UUID, int]:
    id = uuid.uuid4()
    return id, 100


def get_complex_data() -> tuple[
    uuid.UUID,
    int,
    uuid.UUID,
    ipaddress.IPv4Address,
    date,
    time,
    tuple[str, int],
    dict[str, Any],
    set[int],
    relativedelta,
]:
    id = uuid.uuid4()

    tuuid = uuid.UUID("8e14e760-7fa8-11eb-bc66-000000000001")

    ip = ipaddress.IPv4Address("192.168.0.1")

    now = datetime(1, 1, 1, 23, 59, 59, 999000, tzinfo=timezone.utc)
    date = now.date()
    time = now.time()

    tuple_val = (
        "Litwo! Ojczyzno moja! ty jesteś jak zdrowie: Ile cię trzeba cenić, ten tylko się dowie, "
        "Kto cię stracił. Dziś piękność twą w całej ozdobie Widzę i opisuję, bo tęsknię po tobie.",
        1,
    )

    udt = {
        "field1": "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Duis congue egestas sapien id maximus eget.",
        "field2": 4321,
    }

    set_val = {1, 2, 3, 4, 5, 6, 7, 8, 9, 11}

    duration = relativedelta(months=2, days=5, microseconds=36)

    return id, 100, tuuid, ip, date, time, tuple_val, udt, set_val, duration
