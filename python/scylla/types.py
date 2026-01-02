from typing import Final

from ._rust.types import UnsetType  # pyright: ignore[reportMissingModuleSource]

# Singleton instance
Unset: Final[UnsetType] = UnsetType()

# Make UnsetType unimportable in user facing API
del UnsetType

# Export only singleton instance
__all__ = ["Unset"]
