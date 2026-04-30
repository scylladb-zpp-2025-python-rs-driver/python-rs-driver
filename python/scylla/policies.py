from typing import Protocol, runtime_checkable
from ipaddress import IPv4Address, IPv6Address

from ._rust.policies import (  # pyright: ignore[reportMissingModuleSource]
    Authenticator,
    AuthenticatorProvider,
    UntranslatedPeer,
    MonotonicTimestampGenerator,
    SimpleTimestampGenerator,
    AcceptAllHostFilter,
    DcHostFilter,
    Peer,
)


@runtime_checkable
class AddressTranslator(Protocol):
    def translate(self, info: UntranslatedPeer) -> str | tuple[str | IPv4Address | IPv6Address, int]: ...


@runtime_checkable
class TimestampGenerator(Protocol):
    def next_timestamp(self) -> int: ...


@runtime_checkable
class HostFilter(Protocol):
    def accept(self, peer: Peer) -> bool: ...


__all__ = [
    "Authenticator",
    "AuthenticatorProvider",
    "AddressTranslator",
    "UntranslatedPeer",
    "HostFilter",
    "AcceptAllHostFilter",
    "DcHostFilter",
    "TimestampGenerator",
    "MonotonicTimestampGenerator",
    "SimpleTimestampGenerator",
    "Peer",
]
