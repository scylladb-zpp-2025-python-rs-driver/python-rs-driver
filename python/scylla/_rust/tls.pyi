class TlsContext:
    """
    The TlsContext used by the driver.

    This is the actual TLS configuration object. `TlsContextBuilder` builds this object, and then it is passed to the session builder to configure TLS for the session.
    """
    def __init__(
        self,
        *,
        ca_pem: bytes | None = None,
        cert_pem: bytes | None = None,
        key_pem: bytes | None = None,
        verify_peer: bool = True,
    ) -> None: ...
