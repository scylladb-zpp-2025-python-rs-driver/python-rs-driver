"""
Serialization module
"""

import struct
from typing import Any
from dataclasses import dataclass
from scylla._rust.session import PyPreparedStatement
from typing_extensions import override


class SerializationError(Exception):
    pass


@dataclass
class TypeSpec:
    """Column spec and column type representation from rust"""

    kind: str
    name: str
    metadata: dict[str, Any]

    @classmethod
    def from_rust_dict(cls, type_dict: dict[str, Any]) -> "TypeSpec":
        """Create a TypeSpec from dictionary created from Colum spec in rust"""
        kind = type_dict.get("kind", "unknown")
        name = type_dict.get("name", "unknown")

        # Copy all metadata
        metadata = {k: v for k, v in type_dict.items() if k not in ["kind", "name"]}

        return cls(kind=kind, name=name, metadata=metadata)


class TypeSerializer:
    """Base class for type specific serializers"""

    def serialize_raw(self, value: Any) -> bytes:
        """Serialize value to raw bytes"""
        raise NotImplementedError

    def serialize_with_length(self, value: Any) -> bytes:
        """Serialize value with length prefix"""
        if value is None:
            return struct.pack(">i", -1)

        raw_bytes = self.serialize_raw(value)
        return struct.pack(">i", len(raw_bytes)) + raw_bytes


class IntSerializer(TypeSerializer):
    """Serializer for i32"""

    @override
    def serialize_raw(self, value: Any) -> bytes:
        if value is None:
            return b""

        int_val = int(value)
        if not (-2147483648 <= int_val <= 2147483647):
            raise SerializationError(f"Int32 overflow: {int_val}")
        return struct.pack(">i", int_val)


class BigIntSerializer(TypeSerializer):
    """Serializer for i64"""

    @override
    def serialize_raw(self, value: Any) -> bytes:
        if value is None:
            return b""

        long_val = int(value)
        if not (-9223372036854775808 <= long_val <= 9223372036854775807):
            raise SerializationError(f"Int64 overflow: {long_val}")
        return struct.pack(">q", long_val)


class TextSerializer(TypeSerializer):
    """Serializer for text"""

    @override
    def serialize_raw(self, value: Any) -> bytes:
        if value is None:
            return b""
        return str(value).encode("utf-8")


class DoubleSerializer(TypeSerializer):
    """Serializer for double"""

    @override
    def serialize_raw(self, value: Any) -> bytes:
        if value is None:
            return b""
        return struct.pack(">d", float(value))


class BooleanSerializer(TypeSerializer):
    """Serializer for boolean"""

    @override
    def serialize_raw(self, value: Any) -> bytes:
        if value is None:
            return b""
        # Cassandra protocol uses 1 byte for boolean: 0x01 for true, 0x00 for false
        return struct.pack(">B", 1 if bool(value) else 0)


class FloatSerializer(TypeSerializer):
    """Serializer for floats"""

    @override
    def serialize_raw(self, value: Any) -> bytes:
        if value is None:
            return b""
        return struct.pack(">f", float(value))


class ListSerializer(TypeSerializer):
    """Serializer for list collections"""

    def __init__(self, element_serializer: TypeSerializer):
        self.element_serializer = element_serializer

    @override
    def serialize_raw(self, value: Any) -> bytes:
        if value is None or not isinstance(value, (list, tuple)):
            raise SerializationError(f"Expected list/tuple, got {type(value)}")

        # [int32 element_count] followed by each element as [bytes]
        result = bytearray()

        # Write element count
        element_count = len(value)
        result.extend(struct.pack(">i", element_count))

        # Write each element with its own length prefix
        for element in value:
            element_bytes = self.element_serializer.serialize_with_length(element)
            result.extend(element_bytes)

        return bytes(result)


class SetSerializer(TypeSerializer):
    """Serializer for set collections"""

    def __init__(self, element_serializer: TypeSerializer):
        self.element_serializer = element_serializer

    @override
    def serialize_raw(self, value: Any) -> bytes:
        if value is None:
            return b""

        # Convert to list
        if isinstance(value, set):
            value = list(value)
        elif not isinstance(value, (list, tuple)):
            raise SerializationError(f"Expected set/list/tuple, got {type(value)}")

        list_serializer = ListSerializer(self.element_serializer)
        return list_serializer.serialize_raw(value)


class MapSerializer(TypeSerializer):
    """Serializer for map collections"""

    def __init__(
        self, key_serializer: TypeSerializer, value_serializer: TypeSerializer
    ):
        self.key_serializer = key_serializer
        self.value_serializer = value_serializer

    @override
    def serialize_raw(self, value: Any) -> bytes:
        if value is None or not isinstance(value, dict):
            raise SerializationError(f"Expected dict, got {type(value)}")

        # [int32 element_count] followed by key-value pairs
        result = bytearray()

        # Write element count
        element_count = len(value)
        result.extend(struct.pack(">i", element_count))

        # Write each key-value pair
        for k, v in value.items():
            key_bytes = self.key_serializer.serialize_with_length(k)
            value_bytes = self.value_serializer.serialize_with_length(v)
            result.extend(key_bytes)
            result.extend(value_bytes)

        return bytes(result)


class TupleSerializer(TypeSerializer):
    """Serializer for tuple types"""

    def __init__(self, element_serializers: list[TypeSerializer]):
        self.element_serializers = element_serializers

    @override
    def serialize_raw(self, value: Any) -> bytes:
        if value is None or not isinstance(value, (list, tuple)):
            raise SerializationError(f"Expected tuple/list, got {type(value)}")

        if len(value) != len(self.element_serializers):
            raise SerializationError(
                f"Tuple length mismatch: expected {len(self.element_serializers)}, got {len(value)}"
            )

        # Tuples are serialized as consecutive [bytes] values
        result = bytearray()
        for val, serializer in zip(value, self.element_serializers):
            element_bytes = serializer.serialize_with_length(val)
            result.extend(element_bytes)

        return bytes(result)


class UDTSerializer(TypeSerializer):
    """Serializer for User Defined Types (UDTs)"""

    def __init__(self, field_specs: list[tuple[str, TypeSerializer]]):
        """
        field_specs: List of (field_name, field_serializer) tuples in definition order
        """
        self.field_specs = field_specs

    @override
    def serialize_raw(self, value: Any) -> bytes:
        if value is None:
            return b""

        # UDT values can be provided as dict or object with attributes
        if isinstance(value, dict):
            field_values = value
        else:
            # Try to extract values from object attributes or to_dict() method
            if hasattr(value, "to_dict") and callable(getattr(value, "to_dict")):
                field_values = value.to_dict()
            else:
                # Extract from object attributes
                field_values = {}
                for field_name, _ in self.field_specs:
                    if hasattr(value, field_name):
                        field_values[field_name] = getattr(value, field_name)
                    else:
                        raise SerializationError(f"UDT missing field: {field_name}")

        # Serialize fields in definition order
        result = bytearray()
        for field_name, field_serializer in self.field_specs:
            if field_name not in field_values:
                raise SerializationError(f"UDT missing required field: {field_name}")
            field_value = field_values[field_name]
            field_bytes = field_serializer.serialize_with_length(field_value)
            result.extend(field_bytes)

        return bytes(result)


def create_serializer_from_type_spec(type_spec: TypeSpec) -> TypeSerializer:
    """Create a TypeSerializer"""

    if type_spec.kind == "native":
        name = type_spec.name.lower()

        if name in ("int", "integer"):
            return IntSerializer()
        elif name in ("bigint", "long", "counter"):
            return BigIntSerializer()
        elif name in ("text", "varchar", "ascii"):
            return TextSerializer()
        elif name == "double":
            return DoubleSerializer()
        elif name == "float":
            return FloatSerializer()
        elif name == "boolean":
            return BooleanSerializer()
        else:
            raise SerializationError(f"Unsupported native type: {name}")

    elif type_spec.kind == "collection":
        name = type_spec.name

        if name == "list":
            element_type_dict = type_spec.metadata.get("element_type")
            if not element_type_dict:
                raise SerializationError("List type missing element_type")

            element_spec = TypeSpec.from_rust_dict(element_type_dict)
            element_serializer = create_serializer_from_type_spec(element_spec)
            return ListSerializer(element_serializer)

        elif name == "set":
            element_type_dict = type_spec.metadata.get("element_type")
            if not element_type_dict:
                raise SerializationError("Set type missing element_type")

            element_spec = TypeSpec.from_rust_dict(element_type_dict)
            element_serializer = create_serializer_from_type_spec(element_spec)
            return SetSerializer(element_serializer)

        elif name == "map":
            key_type_dict = type_spec.metadata.get("key_type")
            value_type_dict = type_spec.metadata.get("value_type")

            if not key_type_dict or not value_type_dict:
                raise SerializationError("Map type missing key_type or value_type")

            key_spec = TypeSpec.from_rust_dict(key_type_dict)
            value_spec = TypeSpec.from_rust_dict(value_type_dict)

            key_serializer = create_serializer_from_type_spec(key_spec)
            value_serializer = create_serializer_from_type_spec(value_spec)

            return MapSerializer(key_serializer, value_serializer)

        else:
            raise SerializationError(f"Unsupported collection type: {name}")

    elif type_spec.kind == "tuple":
        element_types = type_spec.metadata.get("element_types", [])
        element_serializers = []

        for element_type_dict in element_types:
            element_spec = TypeSpec.from_rust_dict(element_type_dict)
            element_serializer = create_serializer_from_type_spec(element_spec)
            element_serializers.append(element_serializer)

        return TupleSerializer(element_serializers)

    elif type_spec.kind == "user_defined":
        fields_metadata = type_spec.metadata.get("fields")
        if fields_metadata is None:
            raise SerializationError("UDT type missing fields metadata")

        field_specs = []

        for field_info in fields_metadata:
            field_name = field_info.get("name")
            field_type_dict = field_info.get("type")

            if not field_name or not field_type_dict:
                raise SerializationError("UDT field missing name or type")

            field_spec = TypeSpec.from_rust_dict(field_type_dict)
            field_serializer = create_serializer_from_type_spec(field_spec)
            field_specs.append((field_name, field_serializer))

        return UDTSerializer(field_specs)
    else:
        raise SerializationError(f"Unsupported type kind: {type_spec.kind}")


def serialize(values: list[Any], prepared: PyPreparedStatement) -> tuple[bytes, int]:
    """
    Serialize values for a prepared statement using structured type metadata.

    values: List of values to serialize
    prepared: PyPreparedStatement object with type metadata
    """
    try:
        # Avoid circular imports
        import importlib

        session_module = importlib.import_module("scylla.session")
        PyRowSerializationContext = session_module.PyRowSerializationContext

        # Get structured column information from prepared statement
        ctx = PyRowSerializationContext.from_prepared(prepared)
        columns = ctx.columns()

        # Validate value count
        if len(values) != len(columns):
            raise SerializationError(
                f"Value count mismatch: expected {len(columns)} values "
                + f"for columns {[col['name'] for col in columns]}, got {len(values)}"
            )

        # Handle empty values case
        if not values:
            return (b"", 0)

        # Serialize each value
        serialized_data = bytearray()
        for i, (value, column) in enumerate(zip(values, columns)):
            column_name = column["name"]
            type_struct = column.get("type_struct")

            if not type_struct:
                raise SerializationError(
                    f"Column {i} ('{column_name}'): missing type_struct metadata"
                )

            try:
                # Create type spec and serializer
                type_spec = TypeSpec.from_rust_dict(type_struct)
                type_serializer = create_serializer_from_type_spec(type_spec)

                # Serialize
                value_bytes = type_serializer.serialize_with_length(value)
                serialized_data.extend(value_bytes)

            except Exception as e:
                raise SerializationError(f"Column {i} ('{column_name}'): {e}") from e

        return (bytes(serialized_data), len(values))

    except SerializationError:
        # Re-raise SerializationError as-is
        raise
    except Exception as e:
        raise SerializationError(f"Serialization failed: {e}") from e
