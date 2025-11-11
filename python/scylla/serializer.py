"""
Serialization module
"""

import struct
from typing import Any

from typing_extensions import override

from ._rust.statements import *
from ._rust.writers import *
from ._rust.column_type import *

INT32_MIN = -2_147_483_648
INT32_MAX =  2_147_483_647
INT64_MIN = -9_223_372_036_854_775_808
INT64_MAX =  9_223_372_036_854_775_807

class SerializationError(Exception):
    pass

class TypeSerializer:
    """Base class for type specific serializers"""

    def serialize_value(self, value: Any, cell_writer: PyCellWriter) -> None:
        """Serialize value to raw bytes"""
        raise NotImplementedError

    def serialize(self, value: Any, cell_writer: PyCellWriter) -> None:
        if value is None:
            cell_writer.set_null()
            return

        self.serialize_value(value, cell_writer)


class IntSerializer(TypeSerializer):
    """Serializer for i32"""

    @override
    def serialize_value(self, value: Any, cell_writer: PyCellWriter) -> None:

        int_val = int(value)
        if not (INT32_MIN <= int_val <= INT32_MAX):
            raise SerializationError(f"Int32 overflow: {int_val}")

        val_to_bytes = struct.pack(">i", int_val)
        cell_writer.set_value(val_to_bytes)


class BigIntSerializer(TypeSerializer):
    """Serializer for i64"""

    @override
    def serialize_value(self, value: Any, cell_writer: PyCellWriter) -> None:
        long_val = int(value)
        if not (INT64_MIN <= long_val <= INT64_MAX):
            raise SerializationError(f"Int64 overflow: {long_val}")
        val_to_bytes =  struct.pack(">q", long_val)
        cell_writer.set_value(val_to_bytes)


class TextSerializer(TypeSerializer):
    """Serializer for text"""

    @override
    def serialize_value(self, value: Any, cell_writer: PyCellWriter) -> None:
        val_to_bytes = str(value).encode("utf-8")
        cell_writer.set_value(val_to_bytes)

class DoubleSerializer(TypeSerializer):
    """Serializer for double"""

    @override
    def serialize_value(self, value: Any, cell_writer: PyCellWriter) -> None:
        val_to_bytes =  struct.pack(">d", float(value))
        cell_writer.set_value(val_to_bytes)


class BooleanSerializer(TypeSerializer):
    """Serializer for boolean"""

    @override
    def serialize_value(self, value: Any, cell_writer: PyCellWriter) -> None:
        # Cassandra protocol uses 1 byte for boolean: 0x01 for true, 0x00 for false
        val_to_bytes = struct.pack(">B", 1 if bool(value) else 0)
        cell_writer.set_value(val_to_bytes)


class FloatSerializer(TypeSerializer):
    """Serializer for floats"""

    @override
    def serialize_value(self, value: Any, cell_writer: PyCellWriter) -> None:
        val_to_bytes = struct.pack(">f", float(value))
        cell_writer.set_value(val_to_bytes)

class ListSerializer(TypeSerializer):
    """Serializer for list collections"""

    def __init__(self, element_serializer: TypeSerializer):
        self.element_serializer = element_serializer
    @override
    def serialize_value(self, value: Any, cell_writer: PyCellWriter) -> None:
        if not isinstance(value, (list, tuple)):
            raise SerializationError(f"Expected list/tuple, got {type(value)}")

        builder = cell_writer.into_value_builder()

        element_count_bytes = struct.pack(">i", len(value))
        builder.append_bytes(element_count_bytes)

        for element in value:
            self.element_serializer.serialize(element, builder.make_sub_writer())

        builder.finish()

class SetSerializer(TypeSerializer):
    """Serializer for set collections"""

    def __init__(self, element_serializer: TypeSerializer):
        self.element_serializer = element_serializer

    @override
    def serialize_value(self, value: Any, cell_writer: PyCellWriter) -> None:
        # Convert to list
        if isinstance(value, set):
            value = list(value)
        elif not isinstance(value, (list, tuple)):
            raise SerializationError(f"Expected set/list/tuple, got {type(value)}")

        list_serializer = ListSerializer(self.element_serializer)
        list_serializer.serialize_value(value, cell_writer)


class MapSerializer(TypeSerializer):
    """Serializer for map collections"""

    def __init__(
            self, key_serializer: TypeSerializer, value_serializer: TypeSerializer
    ):
        self.key_serializer = key_serializer
        self.value_serializer = value_serializer

    @override
    def serialize_value(self, value: Any, cell_writer: PyCellWriter) -> None:
        if value is None or not isinstance(value, dict):
            raise SerializationError(f"Expected dict, got {type(value)}")

        # [int32 element_count] followed by key-value pairs
        result = bytearray()
        builder = cell_writer.into_value_builder()

        # Write element count
        element_count_bytes = struct.pack(">i", len(value))
        builder.append_bytes(element_count_bytes)

        # Write each key-value pair
        for k, v in value.items():
            self.key_serializer.serialize(k, builder.make_sub_writer())
            self.value_serializer.serialize(v, builder.make_sub_writer())

        builder.finish()


class TupleSerializer(TypeSerializer):
    """Serializer for tuple types"""

    def __init__(self, element_serializers: list[TypeSerializer]):
        self.element_serializers = element_serializers

    @override
    def serialize_value(self, value: Any, cell_writer: PyCellWriter) -> None:
        if value is None or not isinstance(value, (list, tuple)):
            raise SerializationError(f"Expected tuple/list, got {type(value)}")

        if len(value) != len(self.element_serializers):
            raise SerializationError(
                f"Tuple length mismatch: expected {len(self.element_serializers)}, got {len(value)}"
            )

        builder = cell_writer.into_value_builder()

        for val, serializer in zip(value, self.element_serializers):
            serializer.serialize(val, builder.make_sub_writer())

        builder.finish()

class UDTSerializer(TypeSerializer):
    """Serializer for User Defined Types (UDTs)"""

    def __init__(self, field_specs: list[tuple[str, TypeSerializer]]):
        """
        field_specs: List of (field_name, field_serializer) tuples in definition order
        """
        self.field_specs = field_specs

    @override
    def serialize_value(self, value: Any, cell_writer: PyCellWriter) -> None:
        # Handle dict as a "class instance"
        if isinstance(value, dict):
            field_values = value
        else:
            # Default to to_dict if implemented by user
            if hasattr(value, "to_dict") and callable(getattr(value, "to_dict")):
                field_values = value.to_dict()
            else:
                # Extract class attributes
                field_values = {}
                for field_name, _ in self.field_specs:
                    if hasattr(value, field_name):
                        field_values[field_name] = getattr(value, field_name)
                    else:
                        raise SerializationError(f"UDT missing field: {field_name}")

        builder = cell_writer.into_value_builder()

        for field_name, field_serializer in self.field_specs:

            writer = builder.make_sub_writer()
            if field_name not in field_values:
                writer.set_null()
                continue

            field_value = field_values[field_name]
            field_serializer.serialize(field_value, writer)

        builder.finish()

def create_serializer_from_col_type(col_type: Any) -> TypeSerializer:
    """Create a TypeSerializer"""

    if isinstance(col_type, PyNativeType):
        if isinstance(col_type, Int):
            return IntSerializer()
        elif isinstance(col_type, Float):
            return FloatSerializer()
        elif isinstance(col_type, Double):
            return DoubleSerializer()
        elif isinstance(col_type, Text):
            return TextSerializer()
        elif isinstance(col_type, Boolean):
            return BooleanSerializer()
        elif isinstance(col_type, BigInt):
            return BigIntSerializer()
        else:
            raise SerializationError(f"Unsupported native type: {type(col_type).__name__}")
        
    elif isinstance(col_type, PyCollectionType):
        if isinstance(col_type, List):
            element_serializer = create_serializer_from_col_type(col_type.column_type)
            return ListSerializer(element_serializer)

        elif isinstance(col_type, Set):
            element_serializer = create_serializer_from_col_type(col_type.column_type)
            return SetSerializer(element_serializer)

        elif isinstance(col_type, Map):
            key_serializer = create_serializer_from_col_type(col_type.key_type)
            value_serializer = create_serializer_from_col_type(col_type.value_type)
            return MapSerializer(key_serializer, value_serializer)
        else:
            raise SerializationError(f"Unsupported collection type: {type(col_type).__name__}")
        
    elif isinstance(col_type, PyTuple):
        element_types = col_type.element_types
        element_serializers = []

        for element_type in element_types:
            element_serializer = create_serializer_from_col_type(element_type)
            element_serializers.append(element_serializer)

        return TupleSerializer(element_serializers)

    elif isinstance(col_type, PyUserDefinedType):
        fields_metadata = col_type.field_types
        if fields_metadata is None:
            raise SerializationError("UDT type missing fields metadata")

        field_spec = []

        for field_name, field_type in fields_metadata:
            field_serializer = create_serializer_from_col_type(field_type)
            field_spec.append((field_name, field_serializer))

        return UDTSerializer(field_spec)
    
    else:
        raise SerializationError(f"Unsupported type: {type(col_type).__name__}")
class SerializedValues:
    buffer: SerializationBuffer
    ctx: PyRowSerializationContext

    def __init__(self, prepared: PyPreparedStatement):
        self.buffer = SerializationBuffer()
        self.ctx = PyRowSerializationContext.from_prepared(prepared)

    def get_content(self) -> SerializationBuffer:
        return self.buffer

    def add_row(self, values: list[Any]):
        """
        Serialize values for a prepared statement using structured type metadata.

        values: List of values to serialize
        prepared: PyPreparedStatement object with type metadata
        """
        try:
            row_writer = PyRowWriter(self.buffer)
            columns = self.ctx.get_columns()

            if len(values) != len(columns):
                raise SerializationError("Wrong length")

            # Handle empty values case
            if not values:
                raise SerializationError("Serialization failed: values not provided")

            for i, (value, column) in enumerate(zip(values, columns)):
                try:
                    cell_writer = row_writer.make_cell_writer()

                    type_serializer = create_serializer_from_col_type(column)
                    type_serializer.serialize(value, cell_writer)

                except Exception as e:
                    raise SerializationError(f"{e}") from e

            element_count = row_writer.value_count()
            if element_count < 0 or element_count > 65535:
                raise RuntimeError(
                    f"Element count must be in u16 range (0-65535), got {element_count}"
                 )

            self.buffer.set_element_count(element_count)

        except SerializationError:
            raise
        except Exception as e:
            raise SerializationError(f"Serialization failed: {e}") from e