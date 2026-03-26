import ipaddress

from scylla.errors import SessionConfigError
import pytest
from typing import Any
from scylla.session_builder import SessionBuilder


@pytest.mark.asyncio
@pytest.mark.requires_db
@pytest.mark.parametrize(
    "item",
    [
        "127.0.0.2",
        ("127.0.0.2", 9042),
        (ipaddress.IPv4Address("127.0.0.2"), 9042),
        ["127.0.0.2:9042", ("127.0.0.3", 9042), (ipaddress.IPv6Address("::1"), 9042), ("::2", 9042)],
    ],
)
async def test_contact_points_extraction_formats(item: Any):
    builder = SessionBuilder().contact_points(item)
    await builder.connect()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "item",
    [["127.0.0.1", 9042], (None, 9042), ("127.0.0.1", 9042, "extra"), ("127.0.0.2", 999999), ("127.0.0.2", -1)],
)
async def test_contact_points_invalid_types(item: Any):
    builder = SessionBuilder()
    with pytest.raises(SessionConfigError) as excinfo:
        builder.contact_points(item)  # type: ignore[arg-type]

    assert (
        "Invalid contact points type: expected str | tuple(str, int) | tuple(ipaddress, int) or a sequence of these"
        in str(excinfo.value.__cause__)
    )
