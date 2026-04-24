from .enums import Consistency, SerialConsistency

class ExecutionProfile:
    def __init__(
        self,
        timeout: float | None = 30.0,
        consistency: Consistency = Consistency.LocalQuorum,
        serial_consistency: SerialConsistency | None = SerialConsistency.LocalSerial,
    ) -> None: ...
    @property
    def request_timeout(self) -> float | None: ...
    @property
    def consistency(self) -> Consistency: ...
    @property
    def serial_consistency(self) -> SerialConsistency | None: ...
