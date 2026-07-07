from .enums import Consistency, SerialConsistency
from scylla.load_balancing import LoadBalancingPolicy

class ExecutionProfile:
    def __init__(
        self,
        timeout: float | None = 30.0,
        consistency: Consistency = Consistency.LocalQuorum,
        serial_consistency: SerialConsistency | None = SerialConsistency.LocalSerial,
        load_balancing_policy: LoadBalancingPolicy | None = None,
    ) -> None: ...
    @property
    def request_timeout(self) -> float | None: ...
    @property
    def consistency(self) -> Consistency: ...
    @property
    def serial_consistency(self) -> SerialConsistency | None: ...
    @property
    def load_balancing_policy(self) -> LoadBalancingPolicy | None: ...
