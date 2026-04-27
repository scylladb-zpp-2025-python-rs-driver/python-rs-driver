from typing import Protocol, runtime_checkable
from ipaddress import IPv4Address, IPv6Address

from ._rust.policies import (  # pyright: ignore[reportMissingModuleSource]
    Authenticator,
    AuthenticatorProvider,
    UntranslatedPeer,
    MonotonicTimestampGenerator,
    SimpleTimestampGenerator,
    HostFilter,
    Peer,
)


@runtime_checkable
class AddressTranslator(Protocol):
    def translate(self, info: UntranslatedPeer) -> str | tuple[str | IPv4Address | IPv6Address, int]: ...


@runtime_checkable
class TimestampGenerator(Protocol):
    def next_timestamp(self) -> int: ...


__all__ = [
    "Authenticator",
    "AuthenticatorProvider",
    "AddressTranslator",
    "UntranslatedPeer",
    "HostFilter",
    "TimestampGenerator",
    "MonotonicTimestampGenerator",
    "SimpleTimestampGenerator",
    "Peer",
]
