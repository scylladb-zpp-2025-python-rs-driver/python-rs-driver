from .enums import Consistency, SerialConsistency
from .routing import Shard, Token

class RoutingInfo:
    """
    Represents info about statement that can be used by load balancing policies.
    """

    @property
    def consistency(self) -> Consistency:
        """Consistency level for the request."""
        ...

    @property
    def serial_consistency(self) -> SerialConsistency | None:
        """Serial consistency level to be used for serial part of the request, if set."""
        ...

    @property
    def token(self) -> Token | None:
        """
        Token that is the basis of token-aware routing.

        When present, it identifies the token used to choose replicas for
        vnode-based or tablet-based routing.
        """
        ...

    @property
    def keyspace(self) -> str | None:
        """Keyspace that the request is being executed against, if known."""
        ...

    @property
    def table(self) -> str | None:
        """Table that the request is being executed against, if known."""
        ...

    @property
    def is_confirmed_lwt(self) -> bool:
        """
        Whether prepare metadata confirmed that the statement is an LWT.

        If true, load balancing policies can route to replicas in a predefined
        order as a ScyllaDB-specific LWT routing optimisation. This flag alone
        is not sufficient to determine whether a request should be routed as
        LWT: a statement can also use ``Consistency.Serial`` or
        ``Consistency.LocalSerial`` as its consistency level.
        """
        ...

    @property
    def preferred_rack(self) -> str | None:
        """
        Session-level preferred rack to pass to load balancing policies.
        """
        ...

    @property
    def preferred_datacenter(self) -> str | None:
        """
        Session-level preferred datacenter to pass to load balancing policies.
        """
        ...

    def __repr__(self) -> str: ...

