from __future__ import annotations

from collections.abc import Sequence
from ipaddress import IPv4Address, IPv6Address

from .policies import AuthenticatorProvider, AddressTranslator, TimestampGenerator, HostFilter
from .execution_profile import ExecutionProfile
from .session import Session
from typing import Any

Address = str | tuple[str | IPv4Address | IPv6Address, int]

TranslationMap = dict[Any, Any]

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

    def contact_points(self, contact_points: Address | Sequence[Address]) -> SessionBuilder:
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

    def address_translator(self, translator: AddressTranslator | TranslationMap) -> SessionBuilder:
        """
        Registers an address translator for the session.

        The translator can be either a custom Python object implementing the
        :class:`AddressTranslator` protocol or a static translation mapping.

        Parameters
        ----------
        translator : AddressTranslator | dict
            The translation logic to apply. Can be:

            Addresses in the dictionary can be provided as:

            * A string: ``"127.0.0.1:9042"``
            * A tuple: ``("127.0.0.1", 9042)``, ``(IPv4Address("127.0.0.1"), 9042)``, etc.

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
            A custom Python object implementing the :class:`TimestampGenerator`
            protocol.

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
