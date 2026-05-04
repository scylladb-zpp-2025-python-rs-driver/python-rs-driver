from pathlib import Path

from ._rust.tls import TlsContext  # pyright: ignore[reportMissingModuleSource]

__all__ = ["TlsContextBuilder"]


class TlsContextBuilder:
    """
    A builder for creating a TLS context with custom certificate configurations.

    This API allows users to safely mix and match certificates loaded from disk
    and certificates loaded from memory before building the final context.
    """

    def __init__(self, verify_peer: bool = True):
        self._verify_peer = verify_peer
        self._ca_pem: bytes | None = None
        self._cert_pem: bytes | None = None
        self._key_pem: bytes | None = None

    def load_verify_locations(
        self, cafile: str | Path | None = None, cadata: bytes | str | None = None
    ) -> "TlsContextBuilder":
        """
        Load a set of "certification authority" (CA) certificates used to validate
        other peers' certificates.
        """
        if cafile is not None and cadata is not None:
            raise ValueError("cafile and cadata cannot be provided together")

        if cafile is not None:
            self._ca_pem = Path(cafile).read_bytes()
        elif cadata is not None:
            self._ca_pem = cadata if isinstance(cadata, bytes) else cadata.encode("utf-8")

        return self

    def load_cert_chain(
        self,
        certfile: str | Path | None = None,
        keyfile: str | Path | None = None,
        certdata: bytes | str | None = None,
        keydata: bytes | str | None = None,
    ) -> "TlsContextBuilder":
        """
        Load a client certificate and private key for Mutual TLS (mTLS).
        """
        if certfile is not None and certdata is not None:
            raise ValueError("certfile and certdata cannot be provided together")
        if keyfile is not None and keydata is not None:
            raise ValueError("keyfile and keydata cannot be provided together")

        if certfile is not None:
            self._cert_pem = Path(certfile).read_bytes()
        elif certdata is not None:
            self._cert_pem = certdata if isinstance(certdata, bytes) else certdata.encode("utf-8")

        if keyfile is not None:
            self._key_pem = Path(keyfile).read_bytes()
        elif keydata is not None:
            self._key_pem = keydata if isinstance(keydata, bytes) else keydata.encode("utf-8")

        return self

    def build(self) -> TlsContext:
        """
        Compiles the configuration into a `TlsContext` that can be used by the Rust driver.
        The result of this method should be passed to `SessionBuilder.tls_context()`.
        """
        return TlsContext(
            ca_pem=self._ca_pem,
            cert_pem=self._cert_pem,
            key_pem=self._key_pem,
            verify_peer=self._verify_peer,
        )
