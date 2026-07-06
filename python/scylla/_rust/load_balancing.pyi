from collections.abc import Iterable
from .cluster import ClusterState, Node
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

class DefaultPolicy:
    """
    The default load balancing policy.

    It can be configured to be datacenter-aware, rack-aware, and token-aware.
    When the policy is datacenter-aware, you can configure whether to allow
    datacenter failover, which permits sending a query to a node from a remote
    datacenter.

    Node location preferences can be set directly on this policy with
    ``preferred_datacenter`` or ``preferred_datacenter_and_rack``. When no
    policy-level preference is set, the session-level preference is used.
    ``preferred_datacenter_and_rack`` takes precedence over
    ``preferred_datacenter`` when both are set. The effective preference is
    exposed as distinct ``preferred_datacenter`` and ``preferred_rack``
    properties; if only a datacenter is set, ``preferred_rack`` is ``None``.

    Parameters
    ----------
    preferred_datacenter: str | None
        Preferred datacenter for query routing. When set, nodes in this
        datacenter are treated as local. If ``permit_dc_failover`` is false,
        remote nodes are excluded from query plans.
    preferred_datacenter_and_rack: tuple[str, str] | None
        Preferred datacenter and rack for query routing. When set, replicas in
        this rack are preferred first, followed by other nodes in the preferred
        datacenter. This preference includes both datacenter and rack, so it
        takes precedence over ``preferred_datacenter`` when both are set.
    token_aware: bool
        Configures whether the policy takes tokens into consideration when
        creating plans. If this is true and token, keyspace, and table
        information are available, the policy prefers replicas and puts them
        earlier in the query plan.
    permit_dc_failover: bool
        Whether to permit remote nodes, meaning nodes not located in the
        preferred datacenter, in query plans. If no preferred datacenter is set,
        this has no effect.
    enable_shuffling_replicas: bool
        Whether replicas are shuffled when creating query plans. This helps
        distribute load across replicas. Disabling it can make routing more
        deterministic and may improve server-side cache locality.
    """

    def __init__(
        self,
        *,
        preferred_datacenter: str | None = None,
        preferred_datacenter_and_rack: tuple[str, str] | None = None,
        token_aware: bool = True,
        permit_dc_failover: bool = False,
        enable_shuffling_replicas: bool = True,
    ) -> None: ...
    @property
    def preferred_datacenter(self) -> str | None: ...
    @property
    def preferred_rack(self) -> str | None: ...
    @property
    def token_aware(self) -> bool: ...
    @property
    def permit_dc_failover(self) -> bool: ...
    @property
    def enable_shuffling_replicas(self) -> bool: ...
    def pick_targets(
        self,
        routing_info: RoutingInfo,
        cluster_state: ClusterState,
    ) -> Iterable[tuple[Node, Shard | None]]:
        """
        Returns an iterator of ``(Node, shard)`` tuples that are
        the preferred targets for the given request.
        """
        ...

