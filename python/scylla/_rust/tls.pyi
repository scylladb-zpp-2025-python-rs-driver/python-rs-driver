from typing import final

@final
class TlsContext:
    """
    Immutable TLS configuration used by the driver.

    Instances are created by `TlsContextBuilder.build()` and passed to
    `SessionBuilder.tls_context()`.
    """

class TlsContextBuilder:
    """
    Internal Rust-side TLS context builder.

    Users should use the public `TlsContextBuilder` from the Python package.
    """

    def __init__(self) -> None: ...
    def set_verify_peer(self, verify_peer: bool) -> None: ...
    def set_ca_pem(self, ca_pem: bytes) -> None: ...
    def set_cert_chain_pem(
        self,
        cert_pem: bytes,
        key_pem: bytes,
    ) -> None: ...
    def build(self) -> TlsContext: ...
