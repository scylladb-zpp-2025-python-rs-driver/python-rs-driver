import pytest
from typing import Optional, Any, Generator
from scylla.session_builder import SessionBuilder

from scylla.policies import Authenticator

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
