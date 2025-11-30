import pytest
import ipaddress
from scylla.session_builder import SessionBuilder
from typing import List, Dict, Any, cast


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_rows_as_dicts_deserialization():
    """
    Checks that rows_as_dicts() correctly deserializes simple types
    into native Python types.
    """
    builder = SessionBuilder(["127.0.0.2"], 9042)
    session = await builder.connect()

    # Select columns that are known to be of simple types
    query = """
        SELECT key, cluster_name, data_center, rpc_address
        FROM system.local
        LIMIT 1
    """
    result = await session.execute(query)

    # Tell pyright what this returns, and ignore missing type info from the Rust extension
    rows = cast(List[Dict[str, Any]], result.rows_as_dicts())  # type: ignore[attr-defined]

    # It should be a list with one row
    assert isinstance(rows, list)
    assert len(rows) == 1

    row = rows[0]
    assert isinstance(row, dict)

    # Keys should be as expected
    for col in ("key", "cluster_name", "data_center", "rpc_address"):
        assert col in row

    # Types should be native Python types
    assert isinstance(row["key"], str)
    assert isinstance(row["cluster_name"], str)
    assert isinstance(row["data_center"], str)

    # rpc_address should be some kind of IP address representation
    assert (
        isinstance(row["rpc_address"], ipaddress.IPv4Address)
        or isinstance(row["rpc_address"], ipaddress.IPv6Address)
        or isinstance(row["rpc_address"], str)
        or isinstance(row["rpc_address"], bytes)
    )
