import ipaddress
import uuid
from datetime import date, datetime, time, timezone
from typing import Any

from dateutil.relativedelta import relativedelta

SIMPLE_INSERT_QUERY = "INSERT INTO benchmarks.basic (id, val) VALUES (?, ?)"
COMPLEX_INSERT_QUERY = "INSERT INTO benchmarks.complex (id, val, tuuid, ip, date, time, tuple, udt, set1, duration) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
COMPLEX_SELECT_QUERY = "SELECT * FROM benchmarks.complex USING TIMEOUT 120s;"
COMPLEX_SELECT_COUNT = "SELECT COUNT(*) FROM benchmarks.complex USING TIMEOUT 120s;"
SIMPLE_SELECT_COUNT = "SELECT COUNT(*) FROM benchmarks.basic USING TIMEOUT 120s;"
SIMPLE_SELECT_QUERY = "SELECT * FROM benchmarks.basic"


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
    date_val = now.date()
    time_val = now.time()

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

    return id, 100, tuuid, ip, date_val, time_val, tuple_val, udt, set_val, duration
