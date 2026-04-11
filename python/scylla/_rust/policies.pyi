from typing import Any, Optional

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
