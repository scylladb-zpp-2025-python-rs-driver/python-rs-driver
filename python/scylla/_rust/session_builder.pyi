from __future__ import annotations

from collections.abc import Sequence
from ipaddress import IPv4Address, IPv6Address

from .policies import AuthenticatorProvider, AddressTranslator
from .execution_profile import ExecutionProfile
from .session import Session

ContactPoint = str | tuple[str | IPv4Address | IPv6Address, int]

class SessionBuilder:
    """
    Builder for configuring and creating a :class:`Session`.

    The builder exposes a chainable API for setting connection options before
    establishing the session with :meth:`connect`.

    Examples
    --------
    >>> session = await (
    ...     SessionBuilder()
    ...     .contact_points(["127.0.0.1:9042", ("127.0.0.2", 9042)])
    ...     .execution_profile(profile)
    ...     .connect()
    ... )
    """

    def __init__(self) -> None:
        """
        Create a new session builder with default configuration.
        """
        ...

    def contact_points(self, contact_points: ContactPoint | Sequence[ContactPoint]) -> SessionBuilder:
        """
        Set the contact points used to bootstrap the connection.

        Parameters
        ----------
        contact_points : ContactPoint | Sequence[ContactPoint]
            One contact point or a sequence of contact points.

            Each contact point may be provided as:

            - ``str`` — for example ``"127.0.0.1"`` or ``"127.0.0.1:9042"``
            - ``tuple[str, int]`` — for example ``("127.0.0.1", 9042)``
            - ``tuple[IPv4Address | IPv6Address, int]`` — for example
              ``(IPv4Address("127.0.0.1"), 9042)``

        Returns
        -------
        SessionBuilder
        """
        ...

    def execution_profile(self, execution_profile: ExecutionProfile) -> SessionBuilder:
        """
        Set the default execution profile for the session.

        Parameters
        ----------
        execution_profile : ExecutionProfile
            The execution profile to use by default for requests executed through
            the created session.

        Returns
        -------
        SessionBuilder
        """
        ...

    async def connect(self) -> Session:
        """
        Establish a session using the current builder configuration.

        Returns
        -------
        Session
            A connected session ready to execute queries.
        """
        ...

    def user(self, username: str, password: str) -> SessionBuilder:
        """
        Set plain-text credentials for authentication.

        Parameters
        ----------
        username : str
        password : str

        Returns
        -------
        SessionBuilder
        """
        ...

    def authenticator_provider(self, authenticator: AuthenticatorProvider) -> SessionBuilder:
        """
        Set a custom authenticator provider.

        Parameters
        ----------
        authenticator : AuthenticatorProvider
            An instance of a class inheriting from :class:`AuthenticatorProvider`.

        Returns
        -------
        SessionBuilder
        """
        ...

    def address_translator(self, translator: AddressTranslator) -> SessionBuilder:
        """
        Registers a custom Python-defined address translator.

        Parameters
        ----------
        translator : AddressTranslator
            An instance of a class inheriting from :class:`AddressTranslator`.

        Returns
        -------
        SessionBuilder
        """
        ...
