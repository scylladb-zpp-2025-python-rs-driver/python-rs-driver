from pathlib import Path

from ._rust.tls import TlsContext as _RustTlsContext  # pyright: ignore[reportMissingModuleSource, reportPrivateUsage]

__all__ = ["TlsContext"]


class TlsContext:
    """
    Configuration for TLS/SSL connections to the ScyllaDB cluster.

    Note: This class uses static factory methods (@staticmethod) rather than
    a standard __init__ to act as a clear boundary between Python and Rust.
    This allows us to handle Python-specific logic (like file I/O and path
    validation) in pure Python before instantiating the underlying
    native Rust OpenSSL context.
    """

    @staticmethod
    def from_pem(
        *,
        ca_pem: bytes | None = None,
        cert_pem: bytes | None = None,
        key_pem: bytes | None = None,
        verify_peer: bool = True,
    ) -> _RustTlsContext:
        """
        Create a TLS context directly from raw PEM-encoded bytes.
        """
        kwargs = {
            "ca_pem": ca_pem,
            "cert_pem": cert_pem,
            "key_pem": key_pem,
            "verify_peer": verify_peer,
        }
        return _RustTlsContext(**kwargs)  # pyright: ignore[reportCallIssue]

    @staticmethod
    def from_files(
        *,
        ca_path: str | Path | None = None,
        cert_path: str | Path | None = None,
        key_path: str | Path | None = None,
        verify_peer: bool = True,
    ) -> _RustTlsContext:
        """
        Convenience method to load PEM files from disk into the TLS context.
        """
        ca_pem = Path(ca_path).read_bytes() if ca_path else None
        cert_pem = Path(cert_path).read_bytes() if cert_path else None
        key_pem = Path(key_path).read_bytes() if key_path else None

        kwargs = {
            "ca_pem": ca_pem,
            "cert_pem": cert_pem,
            "key_pem": key_pem,
            "verify_peer": verify_peer,
        }
        return _RustTlsContext(**kwargs)  # pyright: ignore[reportCallIssue]
