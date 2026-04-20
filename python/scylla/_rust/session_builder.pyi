from __future__ import annotations

from collections.abc import Sequence
from ipaddress import IPv4Address, IPv6Address

from .policies import AuthenticatorProvider, AddressTranslator, TimestampGenerator, HostFilter
from .execution_profile import ExecutionProfile
from .session import Session
from .enums import Compression, PoolSize
from datetime import timedelta

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

    def shard_aware_local_port_range(self, port_range: tuple[int, int]) -> SessionBuilder:
        """
        Specifies the local port range used for shard-aware connections.

        A possible use case is when you want to have multiple [`Session`] objects and do not want
        them to compete for the ports within the same range. It is then advised to assign
        mutually non-overlapping port ranges to each session object.

        The provided range is inclusive on both ends (i.e. ``[start, end]``).

        By default the driver uses port range ``(49152, 65535)``.

        **Validation Rules:**
        A ``SessionConfigError`` is raised if:
        1. The range is empty (``end`` < ``start``).
        2. The range starts below port ``1024`` (reserved system ports).

        Parameters
        ----------
        port_range : tuple[int, int]
            A tuple of (start_port, end_port), e.g., (49152, 65535).

        Returns
        -------
        SessionBuilder
        """
        ...

    def compression(self, compression: Compression | None) -> SessionBuilder:
        """
        Sets the preferred compression algorithm for the connection.

        By default, no compression is used.

        If the specified compression algorithm is not supported by the
        database server, the session will automatically fall back to
        no compression.

        Parameters
        ----------
        compression : Compression | None
            The compression algorithm to use (e.g., ``Compression.Lz4``),
            or ``None`` to disable compression.

        Returns
        -------
        SessionBuilder
        """
        ...

    def schema_agreement_interval(self, interval: timedelta | float) -> SessionBuilder:
        """
        Sets how often the driver checks for schema agreement.

        The default is 200 milliseconds.

        Parameters
        ----------
        interval : timedelta | float
            The interval duration. If a ``float`` is provided,
            it is interpreted as **seconds**.

        Returns
        -------
        SessionBuilder
        """
        ...

    def tcp_nodelay(self, nodelay: bool) -> SessionBuilder:
        """
        Set the nodelay TCP flag. The default is true.

        Parameters
        ----------
        nodelay : bool

        Returns
        -------
        SessionBuilder
        """
        ...

    def tcp_keepalive_interval(self, timeout: timedelta | float | None) -> SessionBuilder:
        """
        Sets the TCP-level keepalive interval.

        The default is `None`, which implies that no keepalive messages are sent **on TCP layer** when a connection is idle.

        **Note:**
        CQL-layer keepalives are configured separately, with `keepalive_interval`

        Parameters
        ----------
        timeout : timedelta | float | None
            The interval between keepalive probes. If ``float``, interpreted
            as seconds. Set to ``None`` to disable.

        Returns
        -------
        SessionBuilder
        """
        ...

    def use_keyspace(self, keyspace_name: str, case_sensitive: bool) -> SessionBuilder:
        """
        Sets the keyspace to be used for all connections created by this session.

        Each connection created by the driver will automatically execute
        ``USE <keyspace_name>`` before performing any other operations.

        This can be changed later on an active session using ``Session.use_keyspace``.

        Parameters
        ----------
        keyspace_name : str
        case_sensitive : bool

        Returns
        -------
        SessionBuilder
        """
        ...

    def connection_timeout(self, timeout: timedelta | float) -> SessionBuilder:
        """
        Sets the timeout for establishing a new connection to a node. The default is 5 seconds.

        Parameters
        ----------
        timeout : timedelta | float
            The connection timeout. If a ``float`` is provided, it is
            interpreted as **seconds**. Must be non-negative and finite.

        Returns
        -------
        SessionBuilder
        """
        ...

    def pool_size(self, size: PoolSize) -> SessionBuilder:
        """
        Sets the per-node connection pool size.

        The default is one connection per shard, which is the recommended
        setting for Scylla.

        Parameters
        ----------
        size : PoolSize

        Returns
        -------
        SessionBuilder
        """
        ...

    def disallow_shard_aware_port(self, disallow: bool) -> SessionBuilder:
        """
        Controls whether the driver may connect to the shard-aware port.

        By default, shard-aware port connections are allowed. This is a
        Scylla-specific option and usually should not be changed.

        Parameters
        ----------
        disallow : bool

        Returns
        -------
        SessionBuilder
        """
        ...

    def keyspaces_to_fetch(self, keyspaces: Sequence[str]) -> SessionBuilder:
        """
        Sets which keyspaces should be fetched.

        By default, all keyspaces are fetched.

        Parameters
        ----------
        keyspaces : Sequence[str]

        Returns
        -------
        SessionBuilder
        """
        ...

    def fetch_schema_metadata(self, fetch: bool) -> SessionBuilder:
        """
        Controls whether schema metadata should be fetched.

        The default is true.

        Parameters
        ----------
        fetch : bool

        Returns
        -------
        SessionBuilder
        """
        ...

    def metadata_request_serverside_timeout(self, timeout: timedelta | float) -> SessionBuilder:
        """
        Sets the server-side timeout for metadata queries.

        The default is 2 seconds.

        Parameters
        ----------
        timeout : timedelta | float
            The timeout duration. If a ``float`` is provided,
            it is interpreted as **seconds**.

        Returns
        -------
        SessionBuilder
        """
        ...
