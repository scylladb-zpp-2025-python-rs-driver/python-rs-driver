from collections.abc import Sequence

from .execution_profile import ExecutionProfile
from .session import Session

class SessionBuilder:
    def __init__(
        self, contact_points: Sequence[str], port: int, execution_profile: ExecutionProfile | None = None
    ) -> None: ...
    async def connect(self) -> Session: ...
