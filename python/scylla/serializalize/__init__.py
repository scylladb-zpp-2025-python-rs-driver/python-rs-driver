"""
Serialization module
"""

from .serializer import (
    serialize,
    SerializationError,
)

# Export everything that was previously available
__all__ = [
    # Main API
    "serialize",
    "SerializationError",
]


# For backwards compatibility with old imports
