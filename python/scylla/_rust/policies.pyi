import ipaddress
import uuid
from typing import Optional, Tuple, Any
from ipaddress import IPv4Address, IPv6Address

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

class TimestampGenerator:
    """
    Base class for implementing custom client-side timestamp generation.

    Subclass this and override :meth:`next_timestamp` to provide custom logic for generating timestamps for requests.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None: ...
    def next_timestamp(self) -> int:
        """
        Generate the next timestamp for a request.

        This method should return an integer representing the timestamp.

        If this method is not overridden or raises an exception, the
        driver will log the error and fallback to the current system timestamp.

        """
        ...

class Peer:
    """
    Information about a ScyllaDB node discovered by the driver.
    """

    @property
    def host_id(self) -> uuid.UUID: ...
    @property
    def address(self) -> tuple[IPv4Address | IPv6Address, int]: ...
    @property
    def tokens(self) -> list[int]: ...
    @property
    def datacenter(self) -> Optional[str]: ...
    @property
    def rack(self) -> Optional[str]: ...
    def __repr__(self) -> str: ...

class HostFilter:
    """
    Base class for implementing custom host filtering.

    Subclass this and override :meth:`accept` to decide whether a given
    node should be considered by the driver.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None: ...
    def accept(self, peer: Peer) -> bool:
        """
        Decide whether the given peer should be accepted.

        Parameters
        ----------
        peer : Peer
            Information about the node being evaluated.

        Returns
        -------
        bool
            ``True`` if the node should be accepted, ``False`` otherwise.

        If this method is not overridden, raises an exception, or returns
        an invalid value, the driver logs the error and falls back to
        accepting the host.
        """
        ...
