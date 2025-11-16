import struct
from collections.abc import Iterator, Mapping
from typing import Protocol, runtime_checkable

from scylla.serialize import column_type


class SerializationError(Exception):
    pass


class TypeCheckError(SerializationError):
    pass


class SetOrListTypeCheckErrorKind:
    NOT_SET_OR_LIST = "NotSetOrList"


class SetOrListSerializationErrorKind:
    TOO_MANY_ELEMENTS = "TooManyElements"
    ELEMENT_SERIALIZATION_FAILED = "ElementSerializationFailed"


def mk_typck_err_named(
    rust_name: str,
    typ: column_type.ColumnType,
    kind: str,
) -> TypeCheckError:
    """Make a type check error - analogous to Rust's mk_typck_err_named"""
    return TypeCheckError(f"Type check failed for {rust_name}: {kind}, got type {typ}")


def mk_ser_err_named(
    rust_name: str,
    typ: column_type.ColumnType,
    kind: str,
) -> SerializationError:
    """Make a serialization error - analogous to Rust's mk_ser_err_named"""
    return SerializationError(
        f"Serialization failed for {rust_name}: {kind}, for type {typ}"
    )


def exact_type_check_native(
    actual_typ: column_type.ColumnType, expected_native: column_type.Native
) -> None:
    if isinstance(actual_typ, column_type.Native):
        return
    raise TypeCheckError(
        f"Type check failed for {actual_typ}: column_type.Native, got type {expected_native}"
    )


@runtime_checkable
class SerializeValue(Protocol):
    def serialize_value(self, typ: column_type.ColumnType) -> bytes: ...


class Int:
    def __init__(self, value: int):
        if not (-2147483648 <= value <= 2147483647):
            raise TypeCheckError(f"Int overflow: {value}")

        self._value: int = value

    def serialize_value(self, typ: column_type.ColumnType) -> bytes:
        exact_type_check_native(typ, column_type.Native(column_type.NativeType.INT))

        raw_bytes = struct.pack(">i", self._value)
        return struct.pack(">i", 4) + raw_bytes


class BigInt:
    def __init__(self, value: int):
        if not (-9223372036854775808 <= value <= 9223372036854775807):
            raise TypeCheckError(f"BigInt overflow: {value}")

        self._value: int = value

    def serialize_value(self, typ: column_type.ColumnType) -> bytes:
        exact_type_check_native(typ, column_type.Native(column_type.NativeType.BIGINT))

        raw_bytes = struct.pack(">q", self._value)
        return struct.pack(">i", 8) + raw_bytes


class Double:
    def __init__(self, value: float):
        self._value: float = value

    def serialize_value(self, typ: column_type.ColumnType) -> bytes:
        exact_type_check_native(typ, column_type.Native(column_type.NativeType.DOUBLE))

        raw_bytes = struct.pack(">d", self._value)
        return struct.pack(">i", 8) + raw_bytes


class Boolean:
    def __init__(self, value: bool):
        self._value: bool = value

    def serialize_value(self, typ: column_type.ColumnType) -> bytes:
        exact_type_check_native(typ, column_type.Native(column_type.NativeType.BOOLEAN))

        raw_bytes = struct.pack(">B", 1 if self._value else 0)
        return struct.pack(">i", 1) + raw_bytes


class Text:
    def __init__(self, value: str):
        self._value: str = value

    def serialize_value(self, typ: column_type.ColumnType) -> bytes:
        exact_type_check_native(typ, column_type.Native(column_type.NativeType.TEXT))

        raw_bytes = self._value.encode("utf-8")
        return struct.pack(">i", len(raw_bytes)) + raw_bytes


def serialize_sequence(
    rust_name: str,
    length: int,
    iterator: Iterator[SerializeValue],
    typ: column_type.ColumnType,
) -> bytes:
    elt: column_type.ColumnType

    if not (
        isinstance(typ, column_type.Collection)
        and not typ.frozen
        and isinstance(typ, column_type.List)
    ):
        raise mk_typck_err_named(
            rust_name,
            typ,
            SetOrListTypeCheckErrorKind.NOT_SET_OR_LIST,
        )
    elt = typ.element_type
    if length > 0x7FFFFFFF:  # i32::MAX
        raise mk_ser_err_named(
            rust_name,
            typ,
            SetOrListSerializationErrorKind.TOO_MANY_ELEMENTS,
        )

    result = bytearray()
    result.extend(struct.pack(">i", length))

    for el in iterator:
        try:
            element_bytes = el.serialize_value(elt)
            result.extend(element_bytes)
        except Exception as err:
            raise mk_ser_err_named(
                rust_name,
                typ,
                f"{SetOrListSerializationErrorKind.ELEMENT_SERIALIZATION_FAILED}: {err}",
            )

    list_bytes = bytes(result)
    return struct.pack(">i", len(list_bytes)) + list_bytes


class List:
    def __init__(self, elements: list[SerializeValue]):
        self._elements: list[SerializeValue] = elements

    def serialize_value(self, typ: column_type.ColumnType) -> bytes:
        if not isinstance(typ, column_type.Collection) and isinstance(
            typ, column_type.List
        ):
            raise mk_typck_err_named(
                "List",
                typ,
                SetOrListTypeCheckErrorKind.NOT_SET_OR_LIST,
            )

        return serialize_sequence(
            rust_name="List",
            length=len(self._elements),
            iterator=iter(self._elements),
            typ=typ,
        )


class UserDefinedType:
    def __init__(self, field_values: Mapping[str, SerializeValue]):
        self._field_values: Mapping[str, SerializeValue] = field_values

    def serialize_value(self, typ: column_type.ColumnType) -> bytes:
        if not isinstance(typ, column_type.UserDefinedType):
            raise mk_typck_err_named("UserDefinedType", typ, "NotUserDefinedType")

        result = bytearray()
        for field_name, field_type in typ.definition.field_types:
            if field_name not in self._field_values:
                raise mk_ser_err_named(
                    "UserDefinedType", typ, f"MissingRequiredField: {field_name}"
                )

            field_value = self._field_values[field_name]

            try:
                field_bytes = field_value.serialize_value(field_type)
                result.extend(field_bytes)
            except Exception as err:
                raise mk_ser_err_named(
                    "UserDefinedType",
                    typ,
                    f"FieldSerializationFailed: {field_name}: {err}",
                )

        udt_bytes = bytes(result)
        return struct.pack(">i", len(udt_bytes)) + udt_bytes


class ValueList:
    def __init__(self, values: list[SerializeValue]):
        self._values = values

    def serialize(
        self,
        ctx: column_type.RowSerializationContext,
    ) -> tuple[bytearray, int]:
        writer = bytearray()
        if len(self._values) != len(ctx.columns):
            raise SerializationError(
                f"Value count mismatch: expected {len(ctx.columns)} values, "
                f"got {len(self._values)}"
            )

        for i, (value, col_spec) in enumerate(zip(self._values, ctx.columns)):
            if not isinstance(value, SerializeValue):
                raise SerializationError(
                    f"Value at index {i} does not implement SerializeValue protocol"
                )

            try:
                value_bytes = value.serialize_value(col_spec.typ)
                writer.extend(value_bytes)
            except Exception as err:
                raise SerializationError(
                    f"Failed to serialize value at index {i} (column '{col_spec.name}'): {err}"
                ) from err
        element_count = len(self._values)

        if element_count < 0 or element_count > 65535:
            raise SerializationError(
                f"Element count must be in u16 range (0-65535), got {element_count}"
            )

        return writer, element_count

    def is_empty(self) -> bool:
        return len(self._values) == 0
