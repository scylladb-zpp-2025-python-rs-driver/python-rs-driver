from typing import Any
import ipaddress
import uuid
from typing import Optional, Tuple

class Authenticator:
    """
    Base class for implementing custom authentication logic.

    Users should subclass this and override the methods to interface with
    custom auth providers (e.g., LDAP, Kerberos).
    """
    def __init__(self, *args: Any, **kwargs: Any) -> None: ...
    def initial_response(self) -> Optional[bytes]:
        """Return the initial handshake token, or None."""
        ...

    def evaluate_challenge(self, challenge: Optional[bytes]) -> Optional[bytes]:
        """Respond to a server-side authentication challenge."""
        ...

    def success(self, token: Optional[bytes]) -> None:
        """Called when authentication is successful."""
        ...

class AuthenticatorProvider:
    """
    Abstract base class for creating Authenticator instances.
    """
    def __init__(self, *args: Any, **kwargs: Any) -> None: ...
    def new_authenticator(self, authenticator_name: str) -> Authenticator:
        """
        Should return a new instance of an Authenticator subclass.
        """
        ...

class UntranslatedPeer:
    """
    Information about a ScyllaDB node discovered by the driver.
    """

    @property
    def host_id(self) -> uuid.UUID: ...
    @property
    def untranslated_address(self) -> Tuple[ipaddress.IPv4Address | ipaddress.IPv6Address, int]: ...
    @property
    def datacenter(self) -> Optional[str]: ...
    @property
    def rack(self) -> Optional[str]: ...
    def __repr__(self) -> str: ...

class AddressTranslator:
    """
    Base class for implementing custom address translation.
    Subclass this to provide your own translation logic.
    """
    def __init__(self, *args: Any, **kwargs: Any) -> None: ...
    def translate(self, info: UntranslatedPeer) -> Tuple[ipaddress.IPv4Address | ipaddress.IPv6Address, int]:
        """
        Translates a node's address.
        Must return a tuple of (ip_address, port_integer).
        """
        ...
