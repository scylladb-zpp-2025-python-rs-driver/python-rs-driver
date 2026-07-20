from ipaddress import IPv4Address, IPv6Address
from typing import Protocol, runtime_checkable

from ._rust.policies import (  # pyright: ignore[reportMissingModuleSource]
    Authenticator,
    AuthenticatorProvider,
    DictAddressTranslator,
    TimestampGenerator,
    HostFilter,
    Peer,
    UntranslatedPeer,
)


@runtime_checkable
class AddressTranslator(Protocol):
    def translate(self, info: UntranslatedPeer) -> str | tuple[str | IPv4Address | IPv6Address, int]: ...


__all__ = [
    "Authenticator",
    "AuthenticatorProvider",
    "AddressTranslator",
    "UntranslatedPeer",
    "DictAddressTranslator",
    "TimestampGenerator",
    "HostFilter",
    "Peer",
]
