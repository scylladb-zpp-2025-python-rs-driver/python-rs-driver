import shutil
import subprocess
import tempfile
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
from scylla.tls import TlsContextBuilder
from scylla.errors import TlsError


@pytest.fixture(scope="module")
def generated_certs() -> Generator[Path, Any, None]:
    """
    Generates ephemeral TLS certificates for the test session in a temporary
    directory to avoid committing private keys to source control.
    """
    # Check if OpenSSL is available before proceeding, otherwise skip the tests that require it.
    if shutil.which("openssl") is None:
        pytest.skip("OpenSSL is required to run TLS tests")

    # Create a temporary directory that automatically deletes itself later
    with tempfile.TemporaryDirectory() as tmp_dir:
        certs_dir = Path(tmp_dir)

        # Helper to run bash commands
        # We use a list of arguments instead of a single string to avoid potential quoting issues across platforms
        def run_openssl(cmd: list[str]):
            subprocess.run(cmd, check=True, cwd=certs_dir)

        # Generate the CA
        run_openssl(
            [
                "openssl",
                "req",
                "-x509",
                "-newkey",
                "rsa:2048",
                "-keyout",
                "ca.key",
                "-out",
                "ca.crt",
                "-days",
                "1",
                "-nodes",
                "-subj",
                "/CN=Test-CA",
            ]
        )

        # Generate the Server Certificate (for the database)
        run_openssl(
            [
                "openssl",
                "req",
                "-newkey",
                "rsa:2048",
                "-keyout",
                "server.key",
                "-out",
                "server.csr",
                "-nodes",
                "-subj",
                "/CN=127.0.0.1",
            ]
        )

        # Create an extension file for the SAN
        ext_file = certs_dir / "v3.ext"
        ext_file.write_text("subjectAltName=IP:127.0.0.1\n")

        # Attach the ext_file when signing the server cert
        run_openssl(
            [
                "openssl",
                "x509",
                "-req",
                "-in",
                "server.csr",
                "-CA",
                "ca.crt",
                "-CAkey",
                "ca.key",
                "-CAcreateserial",
                "-out",
                "server.crt",
                "-days",
                "1",
                "-extfile",
                ext_file.name,
            ]
        )

        # Generate the Client Certificate (for the Python driver)
        run_openssl(
            [
                "openssl",
                "req",
                "-newkey",
                "rsa:2048",
                "-keyout",
                "client.key",
                "-out",
                "client.csr",
                "-nodes",
                "-subj",
                "/CN=Python-Driver",
            ]
        )
        run_openssl(
            [
                "openssl",
                "x509",
                "-req",
                "-in",
                "client.csr",
                "-CA",
                "ca.crt",
                "-CAkey",
                "ca.key",
                "-CAcreateserial",
                "-out",
                "client.crt",
                "-days",
                "1",
            ]
        )

        yield certs_dir


def test_tls_context_from_pem(generated_certs: Path):
    """Test that we can parse valid PEM bytes."""
    ca_bytes = (generated_certs / "ca.crt").read_bytes()

    context = TlsContextBuilder(verify_peer=False).load_verify_locations(cadata=ca_bytes).build()
    assert context is not None


def test_tls_context_from_files(generated_certs: Path):
    """Test the file loading convenience helper."""
    ca_path = generated_certs / "ca.crt"

    context = TlsContextBuilder(verify_peer=True).load_verify_locations(cafile=ca_path).build()
    assert context is not None


def test_tls_context_missing_key_for_mtls_raises(generated_certs: Path):
    """Test that providing a cert without a key raises our custom TlsError."""
    cert_bytes = (generated_certs / "client.crt").read_bytes()

    with pytest.raises(TlsError) as excinfo:
        TlsContextBuilder(verify_peer=True).load_cert_chain(certdata=cert_bytes).build()

    assert "cert_pem and key_pem must be provided together" in str(excinfo.value)


def test_session_builder_tls_chaining(generated_certs: Path):
    """Test that the builder methods return self and store the config."""
    builder = SessionBuilder()

    context = TlsContextBuilder(verify_peer=True).load_verify_locations(cafile=generated_certs / "ca.crt").build()

    # Test attaching
    assert builder.tls_context(context) is builder

    # Test detaching
    assert builder.tls_context(None) is builder


# --- Integration Tests ---


@pytest.fixture(scope="module")
def tls_ccm_cluster(generated_certs: Path) -> Generator[list[tuple[str, int]], Any, None]:
    """Spins up a local ScyllaDB cluster with TLS enabled."""

    certs_dir = generated_certs

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
@pytest.mark.parametrize("verify_peer", [False, True])
async def test_tls_connection_success(
    tls_ccm_cluster: list[tuple[str, int]],
    generated_certs: Path,
    verify_peer: bool,
):
    """Tests that the driver can securely connect to a TLS-enabled cluster."""
    ca_path = generated_certs / "ca.crt"
    client_cert_path = generated_certs / "client.crt"
    client_key_path = generated_certs / "client.key"

    tls_config = (
        TlsContextBuilder(verify_peer=verify_peer)
        .load_verify_locations(cafile=ca_path)
        .load_cert_chain(certfile=client_cert_path, keyfile=client_key_path)
        .build()
    )

    builder = SessionBuilder().contact_points(tls_ccm_cluster).tls_context(tls_config)

    session = await builder.connect()

    # Prove we can talk to the database
    result = await session.execute("SELECT cluster_name FROM system.local")
    row = await result.first_row()

    assert row is not None
