import ipaddress

from scylla.errors import SessionConfigError
import pytest
from typing import Any, Optional, Generator
from scylla.session_builder import SessionBuilder

from scylla.policies import Authenticator, AuthenticatorProvider, AddressTranslator, UntranslatedPeer


from tests.helpers.ccm import (  # pyright: ignore[reportMissingTypeStubs]
    create_scylla_cluster,
    get_contact_points,
    start_cluster,
    stop_and_remove_cluster,
)


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
