import pytest
from pathlib import Path

from typing import Generator, Any
from tests.helpers.ccm import (  # pyright: ignore[reportMissingTypeStubs]
    create_scylla_cluster,
    get_contact_points,
    start_cluster,
    stop_and_remove_cluster,
)

from scylla.session_builder import SessionBuilder
from scylla.tls import TlsContext
from scylla.errors import TlsError

# Assuming test certificates in the `tests/certs/` directory
CERTS_DIR = Path(__file__).parent / "certs"
CA_PATH = CERTS_DIR / "ca.crt"
CLIENT_CERT_PATH = CERTS_DIR / "client.crt"
CLIENT_KEY_PATH = CERTS_DIR / "client.key"


def test_tls_context_from_pem():
    """Test that we can parse valid PEM bytes."""
    if not CA_PATH.exists():
        pytest.skip("Test certificates not found.")

    ca_bytes = CA_PATH.read_bytes()

    context = TlsContext.from_pem(ca_pem=ca_bytes, verify_peer=False)
    assert context is not None


def test_tls_context_from_files():
    """Test the file loading convenience helper."""
    if not CA_PATH.exists():
        pytest.skip("Test certificates not found.")

    context = TlsContext.from_files(ca_path=CA_PATH, verify_peer=True)
    assert context is not None


def test_tls_context_missing_key_for_mtls_raises():
    """Test that providing a cert without a key raises our custom TlsError."""
    if not CLIENT_CERT_PATH.exists():
        pytest.skip("Test certificates not found.")

    cert_bytes = CLIENT_CERT_PATH.read_bytes()

    with pytest.raises(TlsError) as excinfo:
        TlsContext.from_pem(cert_pem=cert_bytes)

    assert "cert_pem and key_pem must be provided together" in str(excinfo.value)


def test_session_builder_tls_chaining():
    """Test that the builder methods return self and store the config."""
    builder = SessionBuilder()

    if CA_PATH.exists():
        context = TlsContext.from_files(ca_path=CA_PATH)

        # Test attaching
        assert builder.tls_context(context) is builder  # pyright: ignore[reportArgumentType]

        # Test detaching
        assert builder.disable_tls() is builder


# --- Integration Tests ---


@pytest.fixture(scope="module")
def tls_ccm_cluster() -> Generator[list[tuple[str, int]], Any, None]:
    """Spins up a local ScyllaDB cluster with TLS enabled."""

    certs_dir = Path(__file__).parent.resolve() / "certs"

    # This is the exact configuration block ScyllaDB needs to turn on Mutual TLS
    tls_config = {
        "client_encryption_options": {
            "enabled": True,
            "require_client_auth": True,  # Forces the Python driver to provide a client cert
            "certificate": str(certs_dir / "server.crt"),
            "keyfile": str(certs_dir / "server.key"),
            "truststore": str(certs_dir / "ca.crt"),
        }
    }

    cluster = create_scylla_cluster(
        name="tls_cluster",
        scylla_version="release:6.2.2",
        nodes=1,
        config=tls_config,
    )

    start_cluster(cluster)

    try:
        yield get_contact_points(cluster)
    finally:
        stop_and_remove_cluster(cluster)


@pytest.mark.asyncio
@pytest.mark.requires_ccm
async def test_tls_connection_success(tls_ccm_cluster: list[tuple[str, int]]):
    """Tests that the driver can securely connect to a TLS-enabled cluster."""

    tls_config = TlsContext.from_files(
        ca_path=CA_PATH, cert_path=CLIENT_CERT_PATH, key_path=CLIENT_KEY_PATH, verify_peer=False
    )

    builder = SessionBuilder().contact_points(tls_ccm_cluster).tls_context(tls_config)  # pyright: ignore[reportArgumentType]

    session = await builder.connect()

    # Prove we can talk to the database
    result = await session.execute("SELECT cluster_name FROM system.local")
    row = await result.first_row()

    assert row is not None
    print("Successfully communicated over TLS!")
