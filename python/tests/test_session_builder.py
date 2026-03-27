import ipaddress

import pytest
from typing import Optional, Any, Generator
from _pytest.logging import LogCaptureFixture
from scylla.session_builder import SessionBuilder
from scylla.policies import Authenticator, AddressTranslator, PeerInfo, TimestampGenerator

from tests.helpers.ccm import (  # pyright: ignore[reportMissingTypeStubs]
    create_scylla_cluster,
    get_contact_points,
    start_cluster,
    stop_and_remove_cluster,
)


@pytest.fixture(scope="module")
def ccm_contact_points() -> Generator[list[tuple[str, int]], Any, None]:
    cluster = create_scylla_cluster(
        name="auth_cluster",
        scylla_version="release:6.2.2",
        nodes=1,
        config={
            "authenticator": "PasswordAuthenticator",
        },
    )

    start_cluster(cluster)

    try:
        yield get_contact_points(cluster)
    finally:
        stop_and_remove_cluster(cluster)


class MockPlainTextAuthenticator(Authenticator):
    def __init__(self, username: str, password: str):
        super().__init__()
        self.username = username
        self.password = password
        self.challenge_called = False
        self.success_called = False

    def initial_response(self) -> Optional[bytes]:
        return f"\x00{self.username}\x00{self.password}".encode("utf-8")

    def evaluate_challenge(self, challenge: Optional[bytes]) -> Optional[bytes]:
        self.challenge_called = True
        return b""

    def success(self, token: Optional[bytes]) -> None:
        self.success_called = True


class FailingAuthenticator(Authenticator):
    def initial_response(self) -> Optional[bytes]:
        raise RuntimeError("Python Authentication Exploded!")


@pytest.mark.asyncio
@pytest.mark.requires_ccm
async def test_custom_authenticator_success(ccm_contact_points: list[tuple[str, int]]):
    auth = MockPlainTextAuthenticator("cassandra", "cassandra")

    builder = SessionBuilder().contact_points(ccm_contact_points).authenticator_provider(auth)

    session = await builder.connect()

    result = await session.execute("SELECT release_version FROM system.local")
    row = await result.first_row()
    assert row is not None
    assert auth.success_called is True


@pytest.mark.asyncio
@pytest.mark.requires_ccm
async def test_custom_authenticator_failing_python_side(ccm_contact_points: list[tuple[str, int]]):
    auth = FailingAuthenticator()

    builder = SessionBuilder().contact_points(ccm_contact_points).authenticator_provider(auth)

    with pytest.raises(Exception) as excinfo:
        await builder.connect()

    assert "Python Authentication Exploded" in str(excinfo.value)


@pytest.mark.asyncio
@pytest.mark.requires_ccm
async def test_builtin_user_credentials(ccm_contact_points: list[tuple[str, int]]):
    builder = SessionBuilder().contact_points(ccm_contact_points).user("cassandra", "cassandra")

    session = await builder.connect()
    result = await session.execute("SELECT cluster_name FROM system.local")
    assert await result.first_row() is not None


Address = ipaddress.IPv4Address | ipaddress.IPv6Address


class MockAddressTranslator(AddressTranslator):
    default_ip: Address
    default_port: int
    call_log: list[PeerInfo]

    def __init__(self, default_ip: Address, default_port: int) -> None:
        super().__init__()
        self.default_ip = default_ip
        self.default_port = default_port
        self.call_log = []

    def translate(self, info: PeerInfo) -> tuple[Address, int]:
        self.call_log.append(info)
        return self.default_ip, self.default_port


class FailingTranslator(AddressTranslator):
    def translate(self, info: PeerInfo) -> tuple[Address, int]:
        raise RuntimeError("Translation Exploded!")


# 2. Tests
@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_custom_address_translator_discovery():
    translator = MockAddressTranslator(ipaddress.IPv4Address("127.0.0.2"), 9042)

    builder = (
        SessionBuilder()
        .contact_points([("127.0.0.2", 9042)])
        .address_translator(translator)
        .user("cassandra", "cassandra")
    )

    _ = await builder.connect()

    assert len(translator.call_log) > 0, "Translator was never called!"

    translated_ips = [str(p.address[0]) for p in translator.call_log]

    print(f"Nodes seen by translator: {translated_ips}")

    assert "127.0.0.3" in translated_ips or "127.0.0.4" in translated_ips


@pytest.mark.asyncio
@pytest.mark.requires_db
@pytest.mark.xfail(reason="Currently, Python exceptions in the translator do not propagate to the driver")
async def test_address_translator_failing_python_side():
    translator = FailingTranslator()

    builder = (
        SessionBuilder()
        .contact_points([("127.0.0.2", 9042)])
        .user("cassandra", "cassandra")
        .address_translator(translator)
    )

    with pytest.raises(Exception) as excinfo:
        await builder.connect()

    assert "Translation Exploded" in str(excinfo.value)


class MockTimestampGenerator(TimestampGenerator):
    fixed_ts: int
    called: bool

    def __init__(self, fixed_ts: int) -> None:
        super().__init__()
        self.fixed_ts = fixed_ts
        self.called = False

    def next_timestamp(self) -> int:
        self.called = True
        return self.fixed_ts


class FailingTimestampGenerator(TimestampGenerator):
    def next_timestamp(self) -> int:
        raise RuntimeError("Timestamp Generation Exploded!")


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_custom_timestamp_generator_success() -> None:
    my_custom_ts = 1122334455
    ts_gen = MockTimestampGenerator(my_custom_ts)

    builder = (
        SessionBuilder()
        .contact_points([("127.0.0.2", 9042)])
        .user("cassandra", "cassandra")
        .timestamp_generator(ts_gen)
    )

    session = await builder.connect()

    await session.execute(
        "CREATE KEYSPACE IF NOT EXISTS ks WITH REPLICATION = "
        "{'class': 'NetworkTopologyStrategy', 'replication_factor': 1}"
    )
    await session.execute("CREATE TABLE IF NOT EXISTS ks.verify_ts (id int PRIMARY KEY, val text)")
    await session.execute("INSERT INTO ks.verify_ts (id, val) VALUES (99, 'hello')")

    result = await session.execute("SELECT WRITETIME(val) FROM ks.verify_ts WHERE id = 99")
    row = await result.first_row()

    assert row is not None

    db_timestamp = row["writetime(val)"]
    assert db_timestamp == my_custom_ts
    assert ts_gen.called is True


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_custom_timestamp_generator_fallback_on_failure(
    caplog: LogCaptureFixture,
) -> None:
    ts_gen = FailingTimestampGenerator()

    builder = (
        SessionBuilder()
        .contact_points([("127.0.0.2", 9042)])
        .user("cassandra", "cassandra")
        .timestamp_generator(ts_gen)
    )

    session = await builder.connect()

    await session.execute("SELECT now() FROM system.local")

    assert "Failed to generate custom timestamp from Python" in caplog.text
    assert "Timestamp Generation Exploded!" in caplog.text
