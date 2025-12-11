import pytest
from scylla.enums import Consistency, SerialConsistency
from scylla.execution_profile import ExecutionProfile
from scylla.session_builder import SessionBuilder
from scylla.statement import PreparedStatement
from scylla.types import Unset


def test_execution_profile_builder():
    profile = ExecutionProfile()
    assert isinstance(profile, ExecutionProfile)


def test_execution_profile_negative_timeout():
    with pytest.raises(ValueError) as exc_info:
        ExecutionProfile(timeout=-1.0)
    assert "timeout must be a positive, finite number" in str(exc_info.value)


def test_execution_profile_zero_timeout():
    with pytest.raises(ValueError) as exc_info:
        ExecutionProfile(timeout=0.0)
    assert "timeout must be a positive, finite number" in str(exc_info.value)


def test_execution_profile_nan_timeout():
    with pytest.raises(ValueError) as exc_info:
        ExecutionProfile(timeout=float("nan"))
    assert "timeout must be a positive, finite number" in str(exc_info.value)


def test_execution_profile_infinity_timeout():
    with pytest.raises(ValueError) as exc_info:
        ExecutionProfile(timeout=float("inf"))
    assert "timeout must be a positive, finite number" in str(exc_info.value)


def test_execution_profile_builder_consistency():
    expected_consistency = Consistency.One
    profile = ExecutionProfile(consistency=expected_consistency)
    assert isinstance(profile, ExecutionProfile)
    actual_consistency = profile.get_consistency()
    assert actual_consistency != Consistency.Two
    assert actual_consistency == expected_consistency


def test_execution_profile_builder_serial_consistency():
    expected_serial_consistency = SerialConsistency.Serial
    profile = ExecutionProfile(serial_consistency=expected_serial_consistency)
    assert isinstance(profile, ExecutionProfile)
    actual_serial_consistency = profile.get_serial_consistency()
    assert actual_serial_consistency != SerialConsistency.LocalSerial
    assert actual_serial_consistency == expected_serial_consistency


def test_execution_profile_timeout():
    expected_timeout = 10.5
    profile = ExecutionProfile(timeout=expected_timeout)
    assert isinstance(profile, ExecutionProfile)
    actual_timeout = profile.get_request_timeout()
    assert actual_timeout == expected_timeout


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_create_session_with_profile():
    expected_timeout = 10.5
    expected_consistency = Consistency.All
    profile = ExecutionProfile(timeout=expected_timeout, consistency=expected_consistency)
    builder = SessionBuilder(["127.0.0.2"], 9042, execution_profile=profile)
    session = await builder.connect()
    result = await session.execute("SELECT * FROM system.local")
    print(result)


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_invalid_consistency_for_query():
    profile = ExecutionProfile(consistency=Consistency.Three)
    builder = SessionBuilder(["127.0.0.2"], 9042, execution_profile=profile)
    session = await builder.connect()
    with pytest.raises(RuntimeError) as exc_info:
        _ = await session.execute("SELECT * FROM system.local")
        assert "Not enough nodes are alive to satisfy required consistency level" in str(exc_info.value)


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_prepared_with_consistency():
    builder = SessionBuilder(["127.0.0.2"], 9042)
    session = await builder.connect()

    prepared = await session.prepare("SELECT * FROM system.local")
    expected_consistency = Consistency.All
    prepared = prepared.with_consistency(expected_consistency)

    assert isinstance(prepared, PreparedStatement)


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_prepared_with_and_get_consistency():
    builder = SessionBuilder(["127.0.0.2"], 9042)
    session = await builder.connect()

    prepared = await session.prepare("SELECT * FROM system.local")
    expected_consistency = Consistency.All
    prepared = prepared.with_consistency(expected_consistency)

    actual_consistency = prepared.get_consistency()
    assert isinstance(actual_consistency, Consistency)
    assert actual_consistency == expected_consistency


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_prepared_with_and_without_consistency():
    builder = SessionBuilder(["127.0.0.2"], 9042)
    session = await builder.connect()

    prepared = await session.prepare("SELECT * FROM system.local")
    expected_consistency = Consistency.All
    prepared = prepared.with_consistency(expected_consistency)
    prepared = prepared.without_consistency()

    actual_consistency = prepared.get_consistency()
    assert actual_consistency is None


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_prepared_with_execution_profile():
    builder = SessionBuilder(["127.0.0.2"], 9042)
    session = await builder.connect()

    prepared = await session.prepare("SELECT * FROM system.local")
    expected_profile = ExecutionProfile()

    prepared = prepared.with_execution_profile(expected_profile)
    assert isinstance(prepared, PreparedStatement)


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_prepared_with_and_get_execution_profile():
    builder = SessionBuilder(["127.0.0.2"], 9042)
    session = await builder.connect()

    expected_timeout = 1.5
    prepared = await session.prepare("SELECT * FROM system.local")
    expected_profile = ExecutionProfile(timeout=expected_timeout)
    prepared = prepared.with_execution_profile(expected_profile)

    actual_profile = prepared.get_execution_profile()
    assert isinstance(actual_profile, ExecutionProfile)
    assert actual_profile.get_request_timeout() == expected_profile.get_request_timeout()


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_prepared_with_and_without_execution_profile():
    builder = SessionBuilder(["127.0.0.2"], 9042)
    session = await builder.connect()

    expected_timeout = 1.5
    expected_profile = ExecutionProfile(timeout=expected_timeout)
    prepared = await session.prepare("SELECT * FROM system.local")
    prepared = prepared.with_execution_profile(expected_profile)
    prepared = prepared.without_execution_profile()

    actual_profile = prepared.get_execution_profile()
    assert actual_profile is None


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_prepared_with_request_timeout():
    builder = SessionBuilder(["127.0.0.2"], 9042)
    session = await builder.connect()

    prepared = await session.prepare("SELECT * FROM system.local")
    expected_timeout = 10.5
    prepared = prepared.with_request_timeout(expected_timeout)

    assert isinstance(prepared, PreparedStatement)


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_prepared_with_and_get_request_timeout():
    builder = SessionBuilder(["127.0.0.2"], 9042)
    session = await builder.connect()

    prepared = await session.prepare("SELECT * FROM system.local")
    expected_timeout = 10.5
    prepared = prepared.with_request_timeout(expected_timeout)

    actual_timeout = prepared.get_request_timeout()
    assert isinstance(actual_timeout, float)
    assert actual_timeout == expected_timeout


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_prepared_with_and_without_request_timeout():
    builder = SessionBuilder(["127.0.0.2"], 9042)
    session = await builder.connect()

    prepared = await session.prepare("SELECT * FROM system.local")
    expected_timeout = 10.5
    prepared = prepared.with_request_timeout(expected_timeout)
    prepared = prepared.without_request_timeout()

    actual_timeout = prepared.get_request_timeout()
    assert type(actual_timeout) is type(Unset)
    assert actual_timeout is Unset
    assert str(actual_timeout) == "Unset"


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_prepared_with_timeout_set_to_none():
    builder = SessionBuilder(["127.0.0.2"], 9042)
    session = await builder.connect()

    prepared = await session.prepare("SELECT * FROM system.local")
    expected_timeout = None
    prepared = prepared.with_request_timeout(expected_timeout)

    actual_timeout = prepared.get_request_timeout()
    assert actual_timeout is None


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_prepared_with_serial_consistency():
    builder = SessionBuilder(["127.0.0.2"], 9042)
    session = await builder.connect()

    prepared = await session.prepare("SELECT * FROM system.local")
    expected_serial_consistency = SerialConsistency.Serial
    prepared = prepared.with_serial_consistency(expected_serial_consistency)

    assert isinstance(prepared, PreparedStatement)


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_prepared_with_and_get_serial_consistency():
    builder = SessionBuilder(["127.0.0.2"], 9042)
    session = await builder.connect()

    prepared = await session.prepare("SELECT * FROM system.local")
    expected_serial_consistency = SerialConsistency.Serial
    prepared = prepared.with_serial_consistency(expected_serial_consistency)

    actual_serial_consistency = prepared.get_serial_consistency()
    assert isinstance(actual_serial_consistency, SerialConsistency)
    assert actual_serial_consistency == expected_serial_consistency


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_prepared_with_and_without_serial_consistency():
    builder = SessionBuilder(["127.0.0.2"], 9042)
    session = await builder.connect()

    prepared = await session.prepare("SELECT * FROM system.local")
    expected_serial_consistency = SerialConsistency.Serial
    prepared = prepared.with_serial_consistency(expected_serial_consistency)
    prepared = prepared.without_serial_consistency()

    actual_serial_consistency = prepared.get_serial_consistency()
    assert actual_serial_consistency is None
