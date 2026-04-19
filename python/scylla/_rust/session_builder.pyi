from __future__ import annotations

from collections.abc import Sequence
from ipaddress import IPv4Address, IPv6Address

from .policies import AuthenticatorProvider, AddressTranslator, TimestampGenerator, HostFilter
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

    def timestamp_generator(self, generator: TimestampGenerator) -> SessionBuilder:
        """
        Registers a custom Python-defined timestamp generator.

        The generator is used to assign client-side timestamps to requests.
        If the custom generator fails or is not implemented, it will fall back
        to the current system timestamp.

        Parameters
        ----------
        generator : TimestampGenerator
            An instance of a class inheriting from :class:`TimestampGenerator`.

        Returns
        -------
        SessionBuilder
        """
        ...

    def host_filter(self, host_filter: HostFilter) -> SessionBuilder:
        """
        Registers a custom Python-defined host filter.

        The filter is consulted to decide whether a discovered node should be
        accepted by the driver.

        Parameters
        ----------
        host_filter : HostFilter
            An instance of a class inheriting from :class:`HostFilter`.

        Returns
        -------
        SessionBuilder
        """
        ...

    def local_ip_address(self, ip: IPv4Address | IPv6Address | str | None) -> SessionBuilder:
        """
        Sets the local IP address all TCP sockets are bound to.

        By default, this option is set to ``None``, which allows to
        bind to any available address (equivalent to ``INADDR_ANY`` for IPv4
        or ``in6addr_any`` for IPv6).

        Parameters
        ----------
        ip : IPv4Address | IPv6Address | None
            The local IP address to bind to, or ``None`` for the default behavior.

        Returns
        -------
        SessionBuilder
        """
        ...
