from pathlib import Path

class _RustTlsContext:
    """
    A natively compiled Rust OpenSSL context.

    This is the actual TLS configuration object that the Rust driver understands.
    The TlsContext class in tls.py acts as a Python-side builder/factory that
    produces this native context after handling Python-specific logic (like file I/O).
    """
    def __init__(
        self,
        *,
        ca_pem: bytes | None = None,
        cert_pem: bytes | None = None,
        key_pem: bytes | None = None,
        verify_peer: bool = True,
    ) -> None: ...

class TlsContext:
    """
    Configuration for TLS/SSL connections to the ScyllaDB cluster.
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
        Create a native TLS context directly from raw PEM-encoded bytes.

        Parameters
        ----------
        ca_pem : bytes | None, optional
            The PEM-encoded Certificate Authority (CA) chain.
        cert_pem : bytes | None, optional
            The PEM-encoded client certificate for Mutual TLS (mTLS).
        key_pem : bytes | None, optional
            The PEM-encoded client private key for Mutual TLS (mTLS).
        verify_peer : bool, optional
            Whether to strictly verify the server's identity. Default is True.

        Returns
        -------
        _RustTlsContext
            A natively compiled Rust OpenSSL context.
        """
        ...

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

        Parameters
        ----------
        ca_path : str | Path | None, optional
            Path to the Certificate Authority (CA) chain file.
        cert_path : str | Path | None, optional
            Path to the client certificate file.
        key_path : str | Path | None, optional
            Path to the client private key file.
        verify_peer : bool, optional
            Whether to strictly verify the server's identity. Default is True.

        Returns
        -------
        _RustTlsContext
            A natively compiled Rust OpenSSL context.
        """
        ...
