import uuid
from typing import Optional, Any, Protocol, runtime_checkable
from ipaddress import IPv4Address, IPv6Address
from datetime import timedelta

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
    def untranslated_address(self) -> tuple[IPv4Address | IPv6Address, int]: ...
    @property
    def datacenter(self) -> Optional[str]: ...
    @property
    def rack(self) -> Optional[str]: ...
    def __repr__(self) -> str: ...

@runtime_checkable
class AddressTranslator(Protocol):
    """
    Protocol for custom address translation.
    """
    def translate(self, info: UntranslatedPeer) -> str | tuple[str | IPv4Address | IPv6Address, int]:
        """
        Translates a node's address.
        Must return a tuple of (ip_address | str, port_integer) or string with valid address and port.

        When returning a string, it should therefore be a numeric IP address
        plus port (for example ``"127.0.0.1:9042"`` or ``"[::1]:9042"``).
        """
        ...

@runtime_checkable
class TimestampGenerator(Protocol):
    """
    Protocol for custom client-side timestamp generation.
    """
    def next_timestamp(self) -> int:
        """
        Generate the next timestamp for a request.

        This method should return an integer representing the timestamp.

        If this method is not implemented or raises an exception, the
        driver will log the error and fallback to the current system timestamp.

        """
        ...

class SimpleTimestampGenerator:
    """
    A simple client-side timestamp generator based on the system clock.

    This generator returns the current system time in microseconds since
    the Unix Epoch (1970-01-01)
    """
    def __init__(self) -> None: ...
    def next_timestamp(self) -> int: ...

class MonotonicTimestampGenerator:
    """
    Timestamp generator that guarantees monotonically increasing timestamps.

    Parameters
    ----------
    warn_on_drift : bool, default True
        Whether to log warnings when generated timestamps drift too far from
        the system clock.

    warning_threshold : float | timedelta, default 1
        Drift threshold in seconds after which warnings may be emitted.

    warning_interval : float | timedelta, default 1
        Minimum interval in seconds between drift warnings.
    """

    def __init__(
        self,
        warn_on_drift: bool = True,
        warning_threshold: float | timedelta = 1,
        warning_interval: float | timedelta = 1,
    ) -> None: ...
    def next_timestamp(self) -> int: ...

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

@runtime_checkable
class HostFilter(Protocol):
    """
    Protocol for implementing custom host filtering.
    """
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

class AcceptAllHostFilter:
    """
    A host filter that accepts every node in the cluster.
    """
    def __init__(self) -> None: ...
    def accept(self, peer: Peer) -> bool: ...

class DcHostFilter:
    """
    A host filter that accepts nodes only from the specified datacenter.
    """
    def __init__(self, local_dc: str) -> None: ...
    def accept(self, peer: Peer) -> bool: ...
