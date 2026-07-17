from pathlib import Path

import pytest

from scylla.session_builder import SessionBuilder
from scylla.tls import TlsContextBuilder


_TEST_CA_PEM = b"""-----BEGIN CERTIFICATE-----
MIIDKzCCAhOgAwIBAgIUBnidGitucog0MMyMNauh7Lv5td0wDQYJKoZIhvcNAQEL
BQAwJDEiMCAGA1UEAwwZUHl0aG9uIERyaXZlciBUTFMgVGVzdCBDQTAgFw0yNjA3
MTcxMjE5MTRaGA8yMTI2MDYyMzEyMTkxNFowJDEiMCAGA1UEAwwZUHl0aG9uIERy
aXZlciBUTFMgVGVzdCBDQTCCASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoCggEB
ALyFDb/ZyCX81ASfRjJc4yjCoq2l/GAaVZ4HaeFrlZqLf7Dm2xzc5vZbPDWteNqW
Hjp2hxbntpyZA7HWpgJq69l64bKeYFJH8yVlkKPKC/v8smKVrIqULiX4mRkw9B9V
q8Wfk+zQpCxHsGzfRBwg6nfN/V3QDY1x6TtQntH5vj3O9lE3LOllXELydQtCRRUr
PHdqh88CsRArg4dOj3a3ootLLgzCF7vmdqmIhO4qkmFDFLxMEMZRVQqtkekgNlxp
SstxXz57QP3Od6zhcaHFZomKYeV+sme/LsFmj3mrSqM1aNycC4gJJuTVOldRQIjp
uEYzzf1tAOLzmHps76tzYs0CAwEAAaNTMFEwHQYDVR0OBBYEFA7KifO3ptwo1U43
YobrjNXACH8eMB8GA1UdIwQYMBaAFA7KifO3ptwo1U43YobrjNXACH8eMA8GA1Ud
EwEB/wQFMAMBAf8wDQYJKoZIhvcNAQELBQADggEBAImTgyGizsWw9N/gO28vzVFH
J6NevIonI/FV32lPVGlJfr9puWtWn20sYPyUu45YGDZX5S3Rw9Pbi1JNdAKbsjRO
SplL1Mlny3s2H4WjnAskyqygjBEj1OH/xyDLuACipvYaq1KZQC44XhUaHYvrd847
o0qpN+WkmZWFfXex329wJ/opPsAE8NB0H/hQskmDAmcar20kRPacDh3bpRJso4Q8
3Jp7CWkqSGD+BicZExRJg7Tgx9P6OrivgbyvX7+wDrvkkkw23PWWQzPFrwy+x0dO
Bk13uTYki5iliLpf6zKFVzxTnNIWjU4PSo41NeohGEuWw5RVhENepB08WDn5SEc=
-----END CERTIFICATE-----
"""


def test_tls_context_from_pem() -> None:
    """Test loading a valid CA certificate from memory."""
    context = TlsContextBuilder().set_verify_peer(False).load_verify_locations(cadata=_TEST_CA_PEM).build()

    assert context is not None


def test_tls_context_from_file(tmp_path: Path) -> None:
    """Test loading a valid CA certificate from a file."""
    ca_path = tmp_path / "ca.crt"
    ca_path.write_bytes(_TEST_CA_PEM)

    context = TlsContextBuilder().load_verify_locations(cafile=ca_path).build()

    assert context is not None


def test_tls_context_builder_methods_are_chainable() -> None:
    """Test that public TLS builder methods return the builder."""
    builder = TlsContextBuilder()

    assert builder.set_verify_peer(False) is builder
    assert builder.load_verify_locations(cadata=_TEST_CA_PEM) is builder


def test_tls_context_missing_key_for_mtls_raises() -> None:
    """Test that configuring a client certificate requires a private key."""
    with pytest.raises(
        ValueError,
        match="Either keyfile or keydata must be provided",
    ):
        TlsContextBuilder().load_cert_chain(certdata=b"unused")


def test_session_builder_tls_chaining() -> None:
    """Test attaching and detaching a TLS context from SessionBuilder."""
    builder = SessionBuilder()
    context = TlsContextBuilder().set_verify_peer(False).build()

    assert builder.tls_context(context) is builder
    assert builder.tls_context(None) is builder
