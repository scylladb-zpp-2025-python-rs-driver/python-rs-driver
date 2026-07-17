from collections.abc import Generator
from datetime import datetime, timedelta, timezone
from ipaddress import ip_address
from pathlib import Path

import pytest
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives.asymmetric.rsa import RSAPrivateKey
from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID

from scylla.session_builder import SessionBuilder
from scylla.tls import TlsContextBuilder
from tests.helpers.ccm import (  # pyright: ignore[reportMissingTypeStubs]
    create_scylla_cluster,
    get_contact_points,
    start_cluster,
    stop_and_remove_cluster,
)


pytestmark = pytest.mark.requires_ccm

SCYLLA_VERSION = "release:6.2.2"


def _generate_private_key() -> RSAPrivateKey:
    return rsa.generate_private_key(
        public_exponent=65537,
        key_size=2048,
    )


def _certificate_name(common_name: str) -> x509.Name:
    return x509.Name(
        [
            x509.NameAttribute(
                NameOID.COMMON_NAME,
                common_name,
            )
        ]
    )


def _generate_ca_certificate(
    private_key: RSAPrivateKey,
) -> x509.Certificate:
    now = datetime.now(timezone.utc)
    name = _certificate_name("Test CA")

    return (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(name)
        .public_key(private_key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - timedelta(minutes=1))
        .not_valid_after(now + timedelta(days=1))
        .add_extension(
            x509.BasicConstraints(
                ca=True,
                path_length=None,
            ),
            critical=True,
        )
        .add_extension(
            x509.KeyUsage(
                digital_signature=True,
                content_commitment=False,
                key_encipherment=False,
                data_encipherment=False,
                key_agreement=False,
                key_cert_sign=True,
                crl_sign=True,
                encipher_only=False,
                decipher_only=False,
            ),
            critical=True,
        )
        .add_extension(
            x509.SubjectKeyIdentifier.from_public_key(private_key.public_key()),
            critical=False,
        )
        .sign(private_key, hashes.SHA256())
    )


def _generate_leaf_certificate(
    *,
    common_name: str,
    private_key: RSAPrivateKey,
    ca_certificate: x509.Certificate,
    ca_private_key: RSAPrivateKey,
    usage: x509.ObjectIdentifier,
    subject_alternative_name: x509.GeneralName | None = None,
) -> x509.Certificate:
    now = datetime.now(timezone.utc)

    builder = (
        x509.CertificateBuilder()
        .subject_name(_certificate_name(common_name))
        .issuer_name(ca_certificate.subject)
        .public_key(private_key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - timedelta(minutes=1))
        .not_valid_after(now + timedelta(days=1))
        .add_extension(
            x509.BasicConstraints(
                ca=False,
                path_length=None,
            ),
            critical=True,
        )
        .add_extension(
            x509.KeyUsage(
                digital_signature=True,
                content_commitment=False,
                key_encipherment=True,
                data_encipherment=False,
                key_agreement=False,
                key_cert_sign=False,
                crl_sign=False,
                encipher_only=False,
                decipher_only=False,
            ),
            critical=True,
        )
        .add_extension(
            x509.ExtendedKeyUsage([usage]),
            critical=False,
        )
        .add_extension(
            x509.SubjectKeyIdentifier.from_public_key(private_key.public_key()),
            critical=False,
        )
        .add_extension(
            x509.AuthorityKeyIdentifier.from_issuer_public_key(ca_private_key.public_key()),
            critical=False,
        )
    )

    if subject_alternative_name is not None:
        builder = builder.add_extension(
            x509.SubjectAlternativeName([subject_alternative_name]),
            critical=False,
        )

    return builder.sign(
        ca_private_key,
        hashes.SHA256(),
    )


def _write_private_key(
    path: Path,
    private_key: RSAPrivateKey,
) -> None:
    path.write_bytes(
        private_key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        )
    )


def _write_certificate(
    path: Path,
    certificate: x509.Certificate,
) -> None:
    path.write_bytes(
        certificate.public_bytes(
            serialization.Encoding.PEM,
        )
    )


@pytest.fixture(scope="module")
def generated_certs(
    tmp_path_factory: pytest.TempPathFactory,
) -> Path:
    """Generate an ephemeral CA and server and client identities."""
    certs_dir = tmp_path_factory.mktemp("tls-certs")

    ca_private_key = _generate_private_key()
    ca_certificate = _generate_ca_certificate(ca_private_key)

    server_private_key = _generate_private_key()
    server_certificate = _generate_leaf_certificate(
        common_name="127.0.0.1",
        private_key=server_private_key,
        ca_certificate=ca_certificate,
        ca_private_key=ca_private_key,
        usage=ExtendedKeyUsageOID.SERVER_AUTH,
        subject_alternative_name=x509.IPAddress(ip_address("127.0.0.1")),
    )

    client_private_key = _generate_private_key()
    client_certificate = _generate_leaf_certificate(
        common_name="Python Driver",
        private_key=client_private_key,
        ca_certificate=ca_certificate,
        ca_private_key=ca_private_key,
        usage=ExtendedKeyUsageOID.CLIENT_AUTH,
    )

    _write_certificate(certs_dir / "ca.crt", ca_certificate)
    _write_certificate(certs_dir / "server.crt", server_certificate)
    _write_private_key(certs_dir / "server.key", server_private_key)
    _write_certificate(certs_dir / "client.crt", client_certificate)
    _write_private_key(certs_dir / "client.key", client_private_key)

    return certs_dir


@pytest.fixture(scope="module")
def tls_ccm_cluster(
    generated_certs: Path,
) -> Generator[list[tuple[str, int]], None, None]:
    tls_config = {
        "client_encryption_options": {
            "enabled": True,
            "require_client_auth": True,
            "certificate": str(generated_certs / "server.crt"),
            "keyfile": str(generated_certs / "server.key"),
            "truststore": str(generated_certs / "ca.crt"),
        }
    }

    cluster = create_scylla_cluster(
        name="tls_cluster",
        scylla_version=SCYLLA_VERSION,
        nodes=1,
        config=tls_config,
    )

    try:
        start_cluster(cluster)
        yield get_contact_points(cluster)
    finally:
        stop_and_remove_cluster(cluster)


@pytest.mark.asyncio
@pytest.mark.parametrize("verify_peer", [False, True])
async def test_tls_connection_success(
    tls_ccm_cluster: list[tuple[str, int]],
    generated_certs: Path,
    verify_peer: bool,
) -> None:
    """Test connecting to a mutual-TLS-enabled ScyllaDB cluster."""
    tls_context = (
        TlsContextBuilder()
        .set_verify_peer(verify_peer)
        .load_verify_locations(cafile=generated_certs / "ca.crt")
        .load_cert_chain(
            certfile=generated_certs / "client.crt",
            keyfile=generated_certs / "client.key",
        )
        .build()
    )

    session = await SessionBuilder().contact_points(tls_ccm_cluster).tls_context(tls_context).connect()

    result = await session.execute("SELECT cluster_name FROM system.local")
    row = await result.first_row()

    assert row is not None
