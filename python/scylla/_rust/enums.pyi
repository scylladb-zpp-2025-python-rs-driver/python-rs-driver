from enum import IntEnum, Enum
from datetime import timedelta

class Consistency(IntEnum):
    Any = ...
    One = ...
    Two = ...
    Three = ...
    Quorum = ...
    All = ...
    LocalQuorum = ...
    EachQuorum = ...
    LocalOne = ...
    Serial = ...
    LocalSerial = ...

class SerialConsistency(IntEnum):
    Serial = ...
    LocalSerial = ...

class Compression(Enum):
    Lz4 = ...
    Snappy = ...

class PoolSize:
    @staticmethod
    def per_host(connections: int) -> PoolSize:
        """
        Creates a pool size with a fixed number of connections per node.

        Parameters
        ----------
        connections : int
            Number of connections per node. Must be greater than 0.

        Returns
        -------
        PoolSize
        """
        ...

    @staticmethod
    def per_shard(connections: int) -> PoolSize:
        """
        Creates a pool size with a fixed number of connections per shard.

        For Cassandra, nodes are treated as having a single shard.

        The recommended setting for Scylla is ``per_shard(1)``.

        Parameters
        ----------
        connections : int
            Number of connections per shard. Must be greater than 0.

        Returns
        -------
        PoolSize
        """
        ...

class WriteCoalescingDelay:
    @staticmethod
    def small_nondeterministic() -> WriteCoalescingDelay:
        """
        Creates a small nondeterministic delay configuration.

        This is the default setting and is intended for sub-millisecond delays.

        Returns
        -------
        WriteCoalescingDelay
        """
        ...

    @staticmethod
    def from_seconds(delay: float | timedelta) -> WriteCoalescingDelay:
        """
        Creates a delay from a float representing seconds or a timedelta.

        The final value must be greater than 0.

        Parameters
        ----------
        delay : float | timedelta
            The delay duration. If float, it represents seconds.

        Returns
        -------
        WriteCoalescingDelay
        """
        ...

class SelfIdentity:
    def __init__(
        self,
        *,
        custom_driver_name: str | None = None,
        custom_driver_version: str | None = None,
        application_name: str | None = None,
        application_version: str | None = None,
        client_id: str | None = None,
    ) -> None:
        """
        Self-identifying information sent by the driver in the STARTUP message.

        If ``custom_driver_name`` or ``custom_driver_version`` are not provided,
        the driver uses its built-in Python driver name and version.

        Application name, application version, and client ID are not sent unless
        explicitly set.

        Parameters
        ----------
        custom_driver_name : str | None, default None
            Custom driver name to advertise. If ``None``, the built-in Python
            driver name is used.

        custom_driver_version : str | None, default None
            Custom driver version to advertise. If ``None``, the built-in Python
            driver version is used.

        application_name : str | None, default None
            Application name to advertise.

        application_version : str | None, default None
            Application version to advertise.

        client_id : str | None, default None
            Client identifier to advertise.
        """
        ...

    @property
    def custom_driver_name(self) -> str:
        """
        Custom driver name advertised by the driver.
        """
        ...

    @property
    def custom_driver_version(self) -> str:
        """
        Custom driver version advertised by the driver.
        """
        ...

    @property
    def application_name(self) -> str | None:
        """
        Application name advertised by the driver.

        This can be used to distinguish different applications connected to the
        same cluster.
        """
        ...

    @property
    def application_version(self) -> str | None:
        """
        Application version advertised by the driver.
        """
        ...

    @property
    def client_id(self) -> str | None:
        """
        Client identifier advertised by the driver.

        This can be used to distinguish different instances of the same
        application connected to the same cluster.
        """
        ...
