import ipaddress

from scylla.errors import SessionConfigError
import pytest

from typing import Optional, Any, Generator
from _pytest.logging import LogCaptureFixture
from scylla.session_builder import SessionBuilder
from scylla.policies import (
    Authenticator,
    AuthenticatorProvider,
    AddressTranslator,
    UntranslatedPeer,
    TimestampGenerator,
    HostFilter,
    Peer,
)

from tests.helpers.ccm import (  # pyright: ignore[reportMissingTypeStubs]
    create_scylla_cluster,
    get_contact_points,
    start_cluster,
    stop_and_remove_cluster,
)
from datetime import timedelta


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


class SimpleProvider(AuthenticatorProvider):
    def __init__(self, authenticator: Authenticator):
        super().__init__()
        self.auth = authenticator

    def new_authenticator(self, authenticator_name: str) -> Authenticator:
        return self.auth


@pytest.mark.asyncio
@pytest.mark.requires_ccm
async def test_custom_authenticator_success(ccm_contact_points: list[tuple[str, int]]):
    auth = MockPlainTextAuthenticator("cassandra", "cassandra")

    simple_provider = SimpleProvider(auth)

    builder = SessionBuilder().contact_points(ccm_contact_points).authenticator_provider(simple_provider)

    session = await builder.connect()

    result = await session.execute("SELECT release_version FROM system.local")
    row = await result.first_row()
    assert row is not None
    assert auth.success_called is True


@pytest.mark.asyncio
@pytest.mark.requires_ccm
async def test_custom_authenticator_failing_python_side(ccm_contact_points: list[tuple[str, int]]):
    auth = FailingAuthenticator()

    simple_provider = SimpleProvider(auth)

    builder = SessionBuilder().contact_points(ccm_contact_points).authenticator_provider(simple_provider)

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
    call_log: list[UntranslatedPeer]

    def __init__(self, default_ip: Address, default_port: int) -> None:
        super().__init__()
        self.default_ip = default_ip
        self.default_port = default_port
        self.call_log = []

    def translate(self, info: UntranslatedPeer) -> tuple[Address, int]:
        self.call_log.append(info)
        return self.default_ip, self.default_port


class FailingTranslator(AddressTranslator):
    def translate(self, info: UntranslatedPeer) -> tuple[Address, int]:
        raise RuntimeError("Translation Exploded!")


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

    translated_ips = [str(p.untranslated_address[0]) for p in translator.call_log]

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


class AcceptAllHostFilter(HostFilter):
    def __init__(self) -> None:
        super().__init__()
        self.called = False
        self.last_peer_host_id: Optional[object] = None
        self.last_peer_address: Optional[tuple[object, int]] = None

    def accept(self, peer: Peer) -> bool:
        self.called = True
        self.last_peer_host_id = peer.host_id
        self.last_peer_address = peer.address
        return True


class FailingHostFilter(HostFilter):
    def accept(self, peer: Peer) -> bool:
        raise RuntimeError("Host Filter Exploded!")


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_custom_host_filter_success() -> None:
    host_filter = AcceptAllHostFilter()

    builder = (
        SessionBuilder().contact_points([("127.0.0.2", 9042)]).user("cassandra", "cassandra").host_filter(host_filter)
    )

    session = await builder.connect()

    result = await session.execute("SELECT now() FROM system.local")
    row = await result.first_row()

    assert row is not None
    assert host_filter.called is True
    assert host_filter.last_peer_host_id is not None
    assert host_filter.last_peer_address is not None


@pytest.mark.asyncio
@pytest.mark.requires_db
async def test_custom_host_filter_fallback_on_failure(
    caplog: LogCaptureFixture,
) -> None:
    host_filter = FailingHostFilter()

    builder = (
        SessionBuilder().contact_points([("127.0.0.2", 9042)]).user("cassandra", "cassandra").host_filter(host_filter)
    )

    session = await builder.connect()

    result = await session.execute("SELECT now() FROM system.local")
    row = await result.first_row()

    assert row is not None
    assert "Failed to evaluate custom host filter from Python" in caplog.text
    assert "Host Filter Exploded!" in caplog.text


@pytest.mark.parametrize(
    "ip",
    [
        "127.0.0.1",
        "::1",
        ipaddress.IPv4Address("127.0.0.2"),
        ipaddress.IPv6Address("::2"),
        None,
    ],
)
def test_local_ip_address_valid_formats(ip: Any):
    builder = SessionBuilder().local_ip_address(ip)
    assert isinstance(builder, SessionBuilder)


@pytest.mark.parametrize(
    "bad_range",
    [
        ((2000, 1000)),
        ((80, 2000)),
        ((1023, 1024)),
    ],
)
def test_port_range_validation_logic(bad_range: tuple[int, int]):
    builder = SessionBuilder()
    with pytest.raises(SessionConfigError) as excinfo:
        builder.shard_aware_local_port_range(bad_range)

    assert "Invalid port range" in str(excinfo.value)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "valid_range",
    [
        ((1024, 2000)),
        ((1024, 1024)),
    ],
)
async def test_port_range_boundary_valid(valid_range: tuple[int, int]):
    builder = SessionBuilder().shard_aware_local_port_range(valid_range)
    assert isinstance(builder, SessionBuilder)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "valid_duration",
    [
        0.5,
        5,
        timedelta(milliseconds=200),
        timedelta(seconds=2, microseconds=500),
        0.0,
    ],
)
async def test_schema_agreement_interval_happy_path(valid_duration: Any):
    builder = SessionBuilder().schema_agreement_interval(valid_duration)
    assert isinstance(builder, SessionBuilder)


@pytest.mark.parametrize(
    "invalid_input",
    [
        -1.0,
        float("inf"),
    ],
)
def test_schema_agreement_interval_error_consistency(invalid_input: Any):
    builder = SessionBuilder()
    with pytest.raises(SessionConfigError) as excinfo:
        builder.schema_agreement_interval(invalid_input)

    assert "Expected a datetime.timedelta or a non-negative finite float (seconds)" in str(excinfo.value)


def test_tcp_keepalive_warnings(
    caplog: LogCaptureFixture,
):
    _ = SessionBuilder().tcp_keepalive_interval(0.5)
    assert "Setting the TCP keepalive interval to low values" in caplog.text
