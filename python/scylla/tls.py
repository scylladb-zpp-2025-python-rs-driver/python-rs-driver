from pathlib import Path

from ._rust.tls import (  # pyright: ignore[reportMissingModuleSource]
    TlsContext,
    TlsContextBuilder as _RustTlsContextBuilder,
)

__all__ = ["TlsContextBuilder", "TlsContext"]


class TlsContextBuilder:
    """
    A builder for creating a TLS context with custom certificate configurations.

    The builder accepts certificates loaded from files or directly from memory.
    Call `build()` to create the immutable `TlsContext` used by the driver.
    """

    _inner: _RustTlsContextBuilder

    def __init__(self) -> None:
        self._inner = _RustTlsContextBuilder()

    def set_verify_peer(self, verify_peer: bool) -> "TlsContextBuilder":
        """
        Enable or disable verification of the server certificate.

        Peer verification is enabled by default.

        Disabling verification makes the connection vulnerable to
        man-in-the-middle attacks and should generally only be used in
        controlled development environments.
        """
        self._inner.set_verify_peer(verify_peer)
        return self

    def load_verify_locations(
        self, cafile: str | Path | None = None, cadata: bytes | str | None = None
    ) -> "TlsContextBuilder":
        """
        Load CA certificates used to verify the server certificate.

        Exactly one of `cafile` or `cadata` must be provided.

        `cafile` specifies a PEM file on disk. `cadata` contains one or more
        PEM-encoded CA certificates directly.
        """
        if cafile is not None and cadata is not None:
            raise ValueError("cafile and cadata cannot be provided together")

        if cafile is not None:
            ca_pem = Path(cafile).read_bytes()
        elif cadata is not None:
            ca_pem = cadata if isinstance(cadata, bytes) else cadata.encode("utf-8")
        else:
            raise ValueError("Either cafile or cadata must be provided")

        self._inner.set_ca_pem(ca_pem)
        return self

    def load_cert_chain(
        self,
        certfile: str | Path | None = None,
        keyfile: str | Path | None = None,
        certdata: bytes | str | None = None,
        keydata: bytes | str | None = None,
    ) -> "TlsContextBuilder":
        """
        Load a client certificate chain and private key for mutual TLS.

        The certificate must be provided using exactly one of `certfile` or
        `certdata`. The private key must be provided using exactly one of
        `keyfile` or `keydata`.

        The certificate PEM may contain the leaf certificate followed by
        intermediate certificates.
        """
        if certfile is not None and certdata is not None:
            raise ValueError("certfile and certdata cannot be provided together")
        if keyfile is not None and keydata is not None:
            raise ValueError("keyfile and keydata cannot be provided together")

        if certfile is not None:
            cert_pem = Path(certfile).read_bytes()
        elif certdata is not None:
            cert_pem = certdata if isinstance(certdata, bytes) else certdata.encode("utf-8")
        else:
            raise ValueError("Either certfile or certdata must be provided")

        if keyfile is not None:
            key_pem = Path(keyfile).read_bytes()
        elif keydata is not None:
            key_pem = keydata if isinstance(keydata, bytes) else keydata.encode("utf-8")
        else:
            raise ValueError("Either keyfile or keydata must be provided")

        self._inner.set_cert_chain_pem(cert_pem, key_pem)
        return self

    def build(self) -> TlsContext:
        """
        Build the immutable TLS context.

        The returned context can be passed to `SessionBuilder.tls_context()`.
        """
        return self._inner.build()
