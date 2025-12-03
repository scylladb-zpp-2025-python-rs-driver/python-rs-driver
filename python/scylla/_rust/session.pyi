from .results import RequestResult

class Session:
    async def execute(self, request: str) -> RequestResult: ...
