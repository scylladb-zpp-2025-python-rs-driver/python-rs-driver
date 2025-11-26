from collections.abc import Sequence

from .session import Session

class SessionBuilder:
    def __init__(self, contact_points: Sequence[str], port: int) -> None: ...
    async def connect(self) -> Session: ...
