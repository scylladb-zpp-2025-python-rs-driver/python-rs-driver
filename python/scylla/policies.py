from ipaddress import IPv4Address, IPv6Address
from typing import Protocol, runtime_checkable

from ._rust.policies import (  # pyright: ignore[reportMissingModuleSource]
    AcceptAllHostFilter,
    Authenticator,
    AuthenticatorProvider,
    DcHostFilter,
    DictAddressTranslator,
    MonotonicTimestampGenerator,
    Peer,
    SimpleTimestampGenerator,
    UntranslatedPeer,
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
    "DcHostFilter",
    "AcceptAllHostFilter",
    "HostFilter",
    "DictAddressTranslator",
    "TimestampGenerator",
    "MonotonicTimestampGenerator",
    "SimpleTimestampGenerator",
    "Peer",
]
