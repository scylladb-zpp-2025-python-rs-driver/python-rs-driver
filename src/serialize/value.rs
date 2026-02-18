use std::any::Any;
use std::net::IpAddr;
use std::ops::Deref;
use std::sync::Arc;

use bigdecimal::BigDecimal;
use bigdecimal::num_bigint::BigInt;
use chrono::{DateTime, NaiveDate, NaiveTime, Utc};
use thiserror::Error;
use uuid::Uuid;

use pyo3::prelude::*;
use pyo3::types::{PyAny, PyBytes, PyDict, PyInt, PyList, PyMapping, PySet, PyString, PyTuple};
use pyo3::{Bound, PyErr, PyResult};

use scylla::cluster::metadata::{CollectionType, ColumnType, NativeType, UserDefinedType};
use scylla::serialize::SerializationError;
use scylla::serialize::value::{
    BuiltinSerializationError, BuiltinSerializationErrorKind, BuiltinTypeCheckError,
    BuiltinTypeCheckErrorKind, MapSerializationErrorKind, SerializeValue,
    SetOrListSerializationErrorKind, UdtTypeCheckErrorKind,
};
use scylla::serialize::writers::{CellValueBuilder, CellWriter, WrittenCellProof};
use scylla::value::{
    Counter, CqlDuration, CqlTime, CqlTimestamp, CqlTimeuuid, CqlValue, ValueOverflow,
};

use scylla_cql::serialize::value::{
    VectorSerializationErrorKind, serialize_next_variable_length_elem,
};

/// Wrapper around a Python value (`PyAny`) used for Python → CQL serialization.
///
/// This type performs runtime type inspection and dispatches the value to the
/// appropriate serializer based on the target CQL `ColumnType`.
#[derive(Debug)]
pub(super) struct PyAnyWrapper<'a, 'py>(&'a Bound<'py, PyAny>);

impl<'a, 'py> Deref for PyAnyWrapper<'a, 'py> {
    type Target = &'a Bound<'py, PyAny>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<'a, 'py> PyAnyWrapper<'a, 'py> {
    pub(super) fn new(inner: &'a Bound<'py, PyAny>) -> Self {
        Self(inner)
    }

    #[deny(clippy::wildcard_enum_match_arm)]
    /// Dispatcher that inspects ColumnType and, based on that, forwards the value
    /// to the appropriate serializer.
    fn serialize_arbitrary_value<'b>(
        &self,
        typ: &ColumnType,
        cell_writer: CellWriter<'b>,
    ) -> Result<WrittenCellProof<'b>, SerializationError> {
        match typ {
            ColumnType::Native(native_type) => {
                self.serialize_natives(typ, native_type, cell_writer)
            }

            ColumnType::Collection {
                typ: collection_typ,
                ..
            } => match collection_typ {
                CollectionType::List(_) => {
                    let Ok(list) = PyListWrapper::new(self) else {
                        return Err(SerializationError::new(
                            PythonDriverSerializationError::NotList,
                        ));
                    };

                    list.serialize(typ, cell_writer)
                }

                CollectionType::Map(_, _) => {
                    let Ok(map) = PyMapWrapper::new(self) else {
                        return Err(SerializationError::new(
                            PythonDriverSerializationError::NotMapOrUDT,
                        ));
                    };

                    map.serialize(typ, cell_writer)
                }

                CollectionType::Set(_) => {
                    let Ok(set) = PySetWrapper::new(self) else {
                        return Err(SerializationError::new(
                            PythonDriverSerializationError::NotSet,
                        ));
                    };

                    set.serialize(typ, cell_writer)
                }

                _ => {
                    let name = format!("{:?}", collection_typ);

                    Err(SerializationError::new(
                        PythonDriverSerializationError::UnknownCollectionType(name),
                    ))
                }
            },

            ColumnType::Vector {
                typ: element_typ,
                dimensions,
            } => {
                if let Ok(list) = PyListVectorWrapper::new(self, *dimensions, element_typ) {
                    list.serialize(typ, cell_writer)
                } else if let Ok(tuple) = PyTupleVectorWrapper::new(self, *dimensions, element_typ)
                {
                    tuple.serialize(typ, cell_writer)
                } else {
                    Err(SerializationError::new(
                        PythonDriverSerializationError::NotVector,
                    ))
                }
            }

            // Supports UDTs passed as Python dicts.
            // For Python dataclass instances, convert to dict first (e.g., using dataclasses.asdict()).
            ColumnType::UserDefinedType { definition, .. } => {
                let Ok(dict) = PyUdtWrapper::new(self, definition) else {
                    return Err(SerializationError::new(
                        PythonDriverSerializationError::NotMapOrUDT,
                    ));
                };

                dict.serialize(typ, cell_writer)
            }

            ColumnType::Tuple(elements_types) => {
                let Ok(tuple) = PyTupleWrapper::new(self, elements_types) else {
                    return Err(SerializationError::new(
                        PythonDriverSerializationError::NotTuple,
                    ));
                };

                tuple.serialize(typ, cell_writer)
            }
            _ => {
                let name = self.python_type_name()?;
                let name = name.extract::<String>().map_err(|e| {
                    SerializationError::new(PythonDriverSerializationError::PythonError(e))
                })?;

                Err(SerializationError::new(
                    PythonDriverSerializationError::UnknownColumnType(name),
                ))
            }
        }
    }

    #[deny(clippy::wildcard_enum_match_arm)]
    fn serialize_natives<'b>(
        &self,
        typ: &ColumnType,
        native_type: &NativeType,
        cell_writer: CellWriter<'b>,
    ) -> Result<WrittenCellProof<'b>, SerializationError> {
        match native_type {
            // Integer types.
            NativeType::TinyInt => self.serialize_int::<i8>(typ, cell_writer),
            NativeType::SmallInt => self.serialize_int::<i16>(typ, cell_writer),
            NativeType::Int => self.serialize_int::<i32>(typ, cell_writer),
            NativeType::BigInt => self.serialize_int::<i64>(typ, cell_writer),
            NativeType::Counter => {
                let value = self
                    .cast::<PyInt>()
                    .map_err(|_| self.mismatched_type_error::<Counter>(typ))?
                    .extract::<i64>()
                    .map_err(|_| {
                        SerializationError::new(PythonDriverSerializationError::ValueOverflow)
                    })?;

                let counter = Counter(value);
                counter.serialize(typ, cell_writer)
            }
            NativeType::Varint => self.serialize_native::<BigInt>(typ, cell_writer),

            // Float types.
            NativeType::Float => self.serialize_native::<f32>(typ, cell_writer),
            NativeType::Double => self.serialize_native::<f64>(typ, cell_writer),
            NativeType::Decimal => self.serialize_native::<BigDecimal>(typ, cell_writer),

            // Boolean type.
            NativeType::Boolean => self.serialize_native::<bool>(typ, cell_writer),

            // Text types.
            // TODO: Python allows strings that are not valid in Rust, conversion to `&str` is fallible.
            // This case is currently ignored and should be handled in the future. See: #41
            NativeType::Ascii | NativeType::Text => self.serialize_native::<&str>(typ, cell_writer),

            // Binary data type.
            NativeType::Blob => {
                let value = self
                    .cast::<PyBytes>()
                    .map_err(|_| self.mismatched_type_error::<PyBytes>(typ))?;

                let bytes = value.as_bytes();

                bytes.serialize(typ, cell_writer)
            }

            // Datatime types.
            NativeType::Date => self.serialize_native::<NaiveDate>(typ, cell_writer),
            NativeType::Duration => {
                let months = self
                    .getattr("months")
                    .and_then(|m| m.extract::<i32>())
                    .map_err(|_| self.mismatched_type_error::<CqlDuration>(typ))?;

                let days = self
                    .getattr("days")
                    .and_then(|m| m.extract::<i32>())
                    .map_err(|_| self.mismatched_type_error::<CqlDuration>(typ))?;

                let microseconds = self
                    .getattr("microseconds")
                    .and_then(|m| m.extract::<i64>())
                    .map_err(|_| self.mismatched_type_error::<CqlDuration>(typ))?;

                let nanoseconds = microseconds
                    .checked_mul(1000)
                    .ok_or_else(|| SerializationError::new(ValueOverflow))?;

                let duration = CqlDuration {
                    months,
                    days,
                    nanoseconds,
                };
                duration.serialize(typ, cell_writer)
            }
            NativeType::Time => {
                let value = self
                    .extract::<NaiveTime>()
                    .map_err(|_| self.mismatched_type_error::<CqlTime>(typ))?;

                let time: CqlTime = value
                    .try_into()
                    .map_err(|_| self.mismatched_type_error::<CqlTime>(typ))?;

                time.serialize(typ, cell_writer)
            }
            NativeType::Timestamp => {
                let value = self
                    .extract::<DateTime<Utc>>()
                    .map_err(|_| self.mismatched_type_error::<CqlTimestamp>(typ))?;

                let timestamp: CqlTimestamp = value.into();

                timestamp.serialize(typ, cell_writer)
            }

            // IP address type.
            NativeType::Inet => {
                let value = self
                    .getattr("packed")
                    .map_err(|_| self.mismatched_type_error::<IpAddr>(typ))?;

                let value = value
                    .cast::<PyBytes>()
                    .map_err(|_| self.mismatched_type_error::<IpAddr>(typ))?;

                let bytes = value.as_bytes();

                let ip = match bytes.len() {
                    4 => {
                        let val: [u8; 4] = bytes
                            .try_into()
                            .map_err(|_| self.mismatched_type_error::<IpAddr>(typ))?;
                        IpAddr::from(val)
                    }
                    16 => {
                        let val: [u8; 16] = bytes
                            .try_into()
                            .map_err(|_| self.mismatched_type_error::<IpAddr>(typ))?;
                        IpAddr::from(val)
                    }
                    _ => {
                        return Err(self.mismatched_type_error::<IpAddr>(typ));
                    }
                };

                ip.serialize(typ, cell_writer)
            }

            // UUID types.
            NativeType::Timeuuid => {
                let value = self
                    .getattr("bytes")
                    .map_err(|_| self.mismatched_type_error::<CqlTimeuuid>(typ))?;

                let bytes = value
                    .extract::<[u8; 16]>()
                    .map_err(|_| self.mismatched_type_error::<CqlTimeuuid>(typ))?;

                let timeuuid = CqlTimeuuid::from_bytes(bytes);
                timeuuid.serialize(typ, cell_writer)
            }
            NativeType::Uuid => {
                let value = self
                    .getattr("bytes")
                    .map_err(|_| self.mismatched_type_error::<Uuid>(typ))?;

                let bytes = value
                    .extract::<[u8; 16]>()
                    .map_err(|_| self.mismatched_type_error::<Uuid>(typ))?;

                let uuid = Uuid::from_bytes(bytes);
                uuid.serialize(typ, cell_writer)
            }

            _ => {
                let name = format!("{:?}", native_type);

                Err(SerializationError::new(
                    PythonDriverSerializationError::UnknownNativeType(name),
                ))
            }
        }
    }

    fn serialize_native<'b, T>(
        &self,
        typ: &ColumnType,
        cell_writer: CellWriter<'b>,
    ) -> Result<WrittenCellProof<'b>, SerializationError>
    where
        T: pyo3::FromPyObject<'a, 'py> + SerializeValue,
    {
        self.extract::<T>()
            .map_err(|_| self.mismatched_type_error::<T>(typ))?
            .serialize(typ, cell_writer)
    }

    fn serialize_int<'b, T>(
        &self,
        typ: &ColumnType,
        cell_writer: CellWriter<'b>,
    ) -> Result<WrittenCellProof<'b>, SerializationError>
    where
        T: pyo3::FromPyObject<'a, 'py> + SerializeValue,
    {
        // The cast to `PyInt` is performed to distinguish between two different error cases:
        // `MismatchedType` and `ValueOverflow`.
        self.cast::<PyInt>()
            .map_err(|_| self.mismatched_type_error::<T>(typ))?
            .extract::<T>()
            .map_err(|_| SerializationError::new(PythonDriverSerializationError::ValueOverflow))?
            .serialize(typ, cell_writer)
    }

    fn python_type_name(&self) -> Result<Bound<'py, PyString>, SerializationError> {
        self.get_type()
            .name()
            .map_err(|e| SerializationError::new(PythonDriverSerializationError::PythonError(e)))
    }

    /// Maps Python type to the list of CQL types that this type can serialize as.
    ///
    /// Supports only Python types that serialize to native CQL types.
    /// Returns an error if the Python type does not correspond to a native CQL type.
    fn map_type_to_expected(&self) -> Result<&'static [ColumnType<'static>], SerializationError> {
        let name = self.python_type_name()?;

        let name = name
            .extract::<&str>()
            .map_err(|e| SerializationError::new(PythonDriverSerializationError::PythonError(e)))?;

        let columns = match name {
            "int" => INT_COLUMNS,
            "float" => FLOAT_COLUMNS,
            "bool" => BOOL_COLUMNS,
            "str" => STRING_COLUMNS,
            "bytes" => BYTES_COLUMNS,
            "Decimal" => DECIMAL_COLUMNS,
            "relativedelta" => RELATIVEDELTA_COLUMNS,
            "datetime" => DATETIME_COLUMNS,
            "IPv4Address" | "IPv6Address" => IP_COLUMNS,
            "time" => TIME_COLUMNS,
            "UUID" => UUID_COLUMNS,

            _ => {
                return Err(SerializationError::new(
                    PythonDriverSerializationError::UnknownColumnType(name.into()),
                ));
            }
        };

        Ok(columns)
    }

    fn mismatched_type_error<T: ?Sized>(&self, typ: &ColumnType) -> SerializationError {
        let expected = match self.map_type_to_expected() {
            Ok(expected) => expected,
            Err(e) => return e,
        };
        SerializationError::new(mk_typck_err::<T>(
            typ,
            BuiltinTypeCheckErrorKind::MismatchedType { expected },
        ))
    }
}

impl<'a, 'py> SerializeValue for PyAnyWrapper<'a, 'py> {
    fn serialize<'b>(
        &self,
        typ: &ColumnType,
        cell_writer: CellWriter<'b>,
    ) -> Result<WrittenCellProof<'b>, SerializationError> {
        if self.is_none() {
            return Ok(cell_writer.set_null());
        }

        self.serialize_arbitrary_value(typ, cell_writer)
    }
}

fn serialize_sequence<'t, 'b, 'py, T: Any>(
    len: usize,
    iter: impl Iterator<Item = Bound<'py, PyAny>>,
    typ: &ColumnType,
    writer: CellWriter<'b>,
) -> Result<WrittenCellProof<'b>, SerializationError> {
    let ColumnType::Collection {
        typ: CollectionType::List(elt) | CollectionType::Set(elt),
        ..
    } = typ
    else {
        unreachable!("typ is guaranteed to be Set or List");
    };

    let mut builder = writer.into_value_builder();

    let element_count: i32 = len
        .try_into()
        .map_err(|_| mk_ser_err::<T>(typ, SetOrListSerializationErrorKind::TooManyElements))?;
    builder.append_bytes(&element_count.to_be_bytes());

    for el in iter {
        PyAnyWrapper::serialize(&PyAnyWrapper::new(&el), elt, builder.make_sub_writer()).map_err(
            |err| {
                mk_ser_err::<T>(
                    typ,
                    SetOrListSerializationErrorKind::ElementSerializationFailed(err),
                )
            },
        )?;
    }

    builder
        .finish()
        .map_err(|_| mk_ser_err::<T>(typ, BuiltinSerializationErrorKind::SizeOverflow))
}

pub fn serialize_vector<'t, 'b, 'py, T: Any>(
    len: usize,
    iter: impl Iterator<Item = Bound<'py, PyAny>>,
    element_type: &ColumnType,
    dimensions: u16,
    typ: &ColumnType,
    writer: CellWriter<'b>,
) -> Result<WrittenCellProof<'b>, SerializationError> {
    if len != dimensions as usize {
        return Err(mk_ser_err::<T>(
            typ,
            VectorSerializationErrorKind::InvalidNumberOfElements(len, dimensions),
        ));
    }
    let mut builder = writer.into_value_builder();
    match element_type.type_size() {
        Some(_) => {
            for element in iter {
                serialize_next_constant_length_elem::<_, T>(
                    element_type,
                    typ,
                    &mut builder,
                    &PyAnyWrapper::new(&element),
                )?;
            }
        }
        None => {
            for element in iter {
                serialize_next_variable_length_elem(
                    std::any::type_name::<T>(),
                    element_type,
                    typ,
                    &mut builder,
                    &PyAnyWrapper::new(&element),
                )?;
            }
        }
    }

    builder
        .finish()
        .map_err(|_| mk_ser_err::<T>(typ, BuiltinSerializationErrorKind::SizeOverflow))
}

fn serialize_next_constant_length_elem<'t, T: SerializeValue + 't, U: Any>(
    element_type: &ColumnType,
    typ: &ColumnType,
    builder: &mut CellValueBuilder,
    element: &'t T,
) -> Result<(), SerializationError> {
    T::serialize(
        element,
        element_type,
        builder.make_sub_writer_without_size(),
    )
    .map_err(|err| {
        mk_ser_err::<U>(
            typ,
            VectorSerializationErrorKind::ElementSerializationFailed(err),
        )
    })?;
    Ok(())
}

#[derive(Debug)]
struct PyListWrapper<'a, 'py> {
    inner: &'a Bound<'py, PyList>,
}

impl<'a, 'py> Deref for PyListWrapper<'a, 'py> {
    type Target = Bound<'py, PyList>;

    fn deref(&self) -> &Self::Target {
        self.inner
    }
}

impl<'a, 'py> PyListWrapper<'a, 'py> {
    fn new(value: &PyAnyWrapper<'a, 'py>) -> PyResult<Self> {
        let list: &Bound<PyList> = value.cast::<PyList>()?;
        Ok(PyListWrapper { inner: list })
    }
}

impl<'a, 'py> SerializeValue for PyListWrapper<'a, 'py> {
    fn serialize<'b>(
        &self,
        typ: &ColumnType,
        cell_writer: CellWriter<'b>,
    ) -> Result<WrittenCellProof<'b>, SerializationError> {
        let items = self.iter();

        serialize_sequence::<PyList>(items.len(), items, typ, cell_writer)
    }
}

struct PySetWrapper<'py, 'a>(&'a Bound<'py, PySet>);

impl<'a, 'py> Deref for PySetWrapper<'a, 'py> {
    type Target = Bound<'py, PySet>;

    fn deref(&self) -> &Self::Target {
        self.0
    }
}

impl<'a, 'py> PySetWrapper<'a, 'py> {
    fn new(value: &PyAnyWrapper<'a, 'py>) -> PyResult<Self> {
        let set: &Bound<PySet> = value.cast::<PySet>()?;
        Ok(PySetWrapper(set))
    }
}

impl<'a, 'py> SerializeValue for PySetWrapper<'a, 'py> {
    fn serialize<'b>(
        &self,
        typ: &ColumnType,
        cell_writer: CellWriter<'b>,
    ) -> Result<WrittenCellProof<'b>, SerializationError> {
        let items = self.iter();

        serialize_sequence::<PySet>(items.len(), items, typ, cell_writer)
    }
}

struct PyMapWrapper<'a, 'py>(&'a Bound<'py, PyMapping>);

impl<'a, 'py> Deref for PyMapWrapper<'a, 'py> {
    type Target = Bound<'py, PyMapping>;

    fn deref(&self) -> &Self::Target {
        self.0
    }
}

impl<'a, 'py> PyMapWrapper<'a, 'py> {
    fn new(value: &PyAnyWrapper<'a, 'py>) -> PyResult<Self> {
        let map: &Bound<PyMapping> = value.cast::<PyMapping>()?;
        Ok(PyMapWrapper(map))
    }
}

impl<'a, 'py> SerializeValue for PyMapWrapper<'a, 'py> {
    fn serialize<'b>(
        &self,
        typ: &ColumnType,
        cell_writer: CellWriter<'b>,
    ) -> Result<WrittenCellProof<'b>, SerializationError> {
        let ColumnType::Collection {
            typ: CollectionType::Map(ktyp, vtyp),
            ..
        } = typ
        else {
            unreachable!("typ is guaranteed to be Map");
        };

        let mut builder = cell_writer.into_value_builder();

        let items = self
            .items()
            .map_err(|e| SerializationError::new(PythonDriverSerializationError::PythonError(e)))?;

        let element_count: i32 = items.len().try_into().map_err(|_| {
            mk_ser_err::<PyMapping>(typ, MapSerializationErrorKind::TooManyElements)
        })?;
        builder.append_bytes(&element_count.to_be_bytes());

        for pair in items {
            let (key, value) = pair
                .extract::<(Bound<'py, PyAny>, Bound<'py, PyAny>)>()
                .map_err(|e| {
                    SerializationError::new(PythonDriverSerializationError::PythonError(e))
                })?;
            PyAnyWrapper::serialize(&PyAnyWrapper::new(&key), ktyp, builder.make_sub_writer())
                .map_err(|err| {
                    mk_ser_err::<PyMapping>(
                        typ,
                        MapSerializationErrorKind::KeySerializationFailed(err),
                    )
                })?;
            PyAnyWrapper::serialize(&PyAnyWrapper::new(&value), vtyp, builder.make_sub_writer())
                .map_err(|err| {
                    mk_ser_err::<PyMapping>(
                        typ,
                        MapSerializationErrorKind::ValueSerializationFailed(err),
                    )
                })?;
        }

        builder
            .finish()
            .map_err(|_| mk_ser_err::<PyMapping>(typ, BuiltinSerializationErrorKind::SizeOverflow))
    }
}

#[derive(Debug)]
struct PyListVectorWrapper<'py, 'a> {
    inner: &'a Bound<'py, PyList>,
    dimension: u16,
    element_type: &'a ColumnType<'a>,
}

impl<'py, 'a> Deref for PyListVectorWrapper<'py, 'a> {
    type Target = Bound<'py, PyList>;

    fn deref(&self) -> &Self::Target {
        self.inner
    }
}

impl<'py, 'a> PyListVectorWrapper<'py, 'a> {
    fn new(
        value: &PyAnyWrapper<'a, 'py>,
        dimension: u16,
        element_type: &'a ColumnType<'a>,
    ) -> PyResult<Self> {
        let list: &Bound<PyList> = value.cast::<PyList>()?;
        Ok(PyListVectorWrapper {
            inner: list,
            dimension,
            element_type,
        })
    }
}

impl<'py, 'a> SerializeValue for PyListVectorWrapper<'py, 'a> {
    fn serialize<'b>(
        &self,
        typ: &ColumnType,
        cell_writer: CellWriter<'b>,
    ) -> Result<WrittenCellProof<'b>, SerializationError> {
        let items = self.iter();

        serialize_vector::<PyList>(
            items.len(),
            items,
            self.element_type,
            self.dimension,
            typ,
            cell_writer,
        )
    }
}

#[derive(Debug)]
struct PyTupleVectorWrapper<'py, 'a> {
    inner: Bound<'py, PyTuple>,
    dimension: u16,
    element_type: &'a ColumnType<'a>,
}

impl<'py, 'a> Deref for PyTupleVectorWrapper<'py, 'a> {
    type Target = Bound<'py, PyTuple>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<'py, 'a> PyTupleVectorWrapper<'py, 'a> {
    fn new(
        value: &PyAnyWrapper<'a, 'py>,
        dimension: u16,
        element_type: &'a ColumnType<'a>,
    ) -> PyResult<Self> {
        let tuple: &Bound<PyTuple> = value.cast::<PyTuple>()?;

        Ok(PyTupleVectorWrapper {
            inner: Bound::clone(tuple),
            dimension,
            element_type,
        })
    }
}

impl<'py, 'a> SerializeValue for PyTupleVectorWrapper<'py, 'a> {
    fn serialize<'b>(
        &self,
        typ: &ColumnType,
        cell_writer: CellWriter<'b>,
    ) -> Result<WrittenCellProof<'b>, SerializationError> {
        let items = self.iter();

        serialize_vector::<PyTuple>(
            items.len(),
            items,
            self.element_type,
            self.dimension,
            typ,
            cell_writer,
        )
    }
}

struct PyUdtWrapper<'py, 'a> {
    inner: &'a Bound<'py, PyDict>,
    definition: &'a Arc<UserDefinedType<'a>>,
}

impl<'py> Deref for PyUdtWrapper<'py, '_> {
    type Target = Bound<'py, PyDict>;
    fn deref(&self) -> &Self::Target {
        self.inner
    }
}

impl<'py, 'a> PyUdtWrapper<'py, 'a> {
    fn new(
        value: &PyAnyWrapper<'a, 'py>,
        definition: &'a Arc<UserDefinedType<'a>>,
    ) -> PyResult<Self> {
        let dict: &Bound<PyDict> = value.cast::<PyDict>()?;
        Ok(PyUdtWrapper {
            inner: dict,
            definition,
        })
    }
}

impl<'py> SerializeValue for PyUdtWrapper<'py, '_> {
    fn serialize<'a>(
        &self,
        typ: &ColumnType,
        cell_writer: CellWriter<'a>,
    ) -> Result<WrittenCellProof<'a>, SerializationError> {
        let mut builder = cell_writer.into_value_builder();

        for (field_name, field_type) in &self.definition.field_types {
            let item: Bound<PyAny> = self
                .inner
                .get_item(field_name)
                .map_err(|e| {
                    SerializationError::new(PythonDriverSerializationError::PythonError(e))
                })?
                .ok_or_else(|| {
                    mk_typck_err::<PyDict>(
                        typ,
                        UdtTypeCheckErrorKind::ValueMissingForUdtField {
                            field_name: field_name.to_string(),
                        },
                    )
                })?;

            PyAnyWrapper::new(&item)
                .serialize_arbitrary_value(field_type, builder.make_sub_writer())?;
        }

        builder
            .finish()
            .map_err(|_| mk_ser_err::<CqlValue>(typ, BuiltinSerializationErrorKind::SizeOverflow))
    }
}

#[derive(Debug)]
struct PyTupleWrapper<'py, 'a> {
    inner: &'a Bound<'py, PyTuple>,
    elements_types: &'a Vec<ColumnType<'a>>,
}

impl<'py> Deref for PyTupleWrapper<'py, '_> {
    type Target = Bound<'py, PyTuple>;

    fn deref(&self) -> &Self::Target {
        self.inner
    }
}

impl<'py, 'a> PyTupleWrapper<'py, 'a> {
    fn new(
        value: &PyAnyWrapper<'a, 'py>,
        elements_types: &'a Vec<ColumnType<'_>>,
    ) -> PyResult<Self> {
        let tuple: &Bound<PyTuple> = value.cast::<PyTuple>()?;
        Ok(PyTupleWrapper {
            inner: tuple,
            elements_types,
        })
    }
}

impl<'py, 'a> SerializeValue for PyTupleWrapper<'py, 'a> {
    fn serialize<'b>(
        &self,
        typ: &ColumnType,
        cell_writer: CellWriter<'b>,
    ) -> Result<WrittenCellProof<'b>, SerializationError> {
        let mut builder = cell_writer.into_value_builder();

        for (val, element_type) in self.inner.iter().zip(self.elements_types) {
            PyAnyWrapper::new(&val)
                .serialize_arbitrary_value(element_type, builder.make_sub_writer())?;
        }

        builder
            .finish()
            .map_err(|_| mk_ser_err::<CqlValue>(typ, BuiltinSerializationErrorKind::SizeOverflow))
    }
}

#[derive(Error, Debug)]
pub(crate) enum PythonDriverSerializationError {
    #[error(transparent)]
    PythonError(#[from] PyErr),

    #[error("Unknown native type: {0}")]
    UnknownNativeType(String),

    #[error("Unknown collection type: {0}")]
    UnknownCollectionType(String),

    #[error("Unknown column type: {0}")]
    UnknownColumnType(String),

    #[error("The Python type the CQL type was attempted to be type checked against was not a list")]
    NotList,

    #[error("The Python type the CQL type was attempted to be type checked against was not a set")]
    NotSet,

    #[error("The Python type the CQL type was attempted to be type checked against was not a dict")]
    NotMapOrUDT,

    #[error(
        "The Python type the CQL type was attempted to be type checked against was neither a list, nor a tuple"
    )]
    NotVector,

    #[error(
        "The Python type the CQL type was attempted to be type checked against was not a tuple"
    )]
    NotTuple,

    #[error("The Python value is out of range supported by the CQL typ")]
    ValueOverflow,
}

// List of CQL column types used to provide clear error messages
// indicating which CQL types are compatible with Python `int` type.
static INT_COLUMNS: &[ColumnType<'static>] = &[
    ColumnType::Native(NativeType::TinyInt),
    ColumnType::Native(NativeType::SmallInt),
    ColumnType::Native(NativeType::Int),
    ColumnType::Native(NativeType::BigInt),
    ColumnType::Native(NativeType::Counter),
    ColumnType::Native(NativeType::Varint),
];

// List of CQL column types used to provide clear error messages
// indicating which CQL types are compatible with Python `float` type.
static FLOAT_COLUMNS: &[ColumnType<'static>] = &[
    ColumnType::Native(NativeType::Double),
    ColumnType::Native(NativeType::Float),
];

// List of CQL column types used to provide clear error messages
// indicating which CQL types are compatible with Python `Decimal` type.
static DECIMAL_COLUMNS: &[ColumnType<'static>] = &[ColumnType::Native(NativeType::Decimal)];

// List of CQL column types used to provide clear error messages
// indicating which CQL types are compatible with Python `str` type.
static STRING_COLUMNS: &[ColumnType<'static>] = &[
    ColumnType::Native(NativeType::Ascii),
    ColumnType::Native(NativeType::Text),
];

// List of CQL column types used to provide clear error messages
// indicating which CQL types are compatible with Python `bool` type.
static BOOL_COLUMNS: &[ColumnType<'static>] = &[ColumnType::Native(NativeType::Boolean)];

// List of CQL column types used to provide clear error messages
// indicating which CQL types are compatible with Python `bytes` type.
static BYTES_COLUMNS: &[ColumnType<'static>] = &[ColumnType::Native(NativeType::Blob)];

// List of CQL column types used to provide clear error messages
// indicating which CQL types are compatible with Python `relativedelta` type.
static RELATIVEDELTA_COLUMNS: &[ColumnType<'static>] = &[ColumnType::Native(NativeType::Duration)];

// List of CQL column types used to provide clear error messages
// indicating which CQL types are compatible with Python `datetime.date`
// and `datetime.datetime` types.
static DATETIME_COLUMNS: &[ColumnType<'static>] = &[
    ColumnType::Native(NativeType::Date),
    ColumnType::Native(NativeType::Timestamp),
];

// List of CQL column types used to provide clear error messages
// indicating which CQL types are compatible with Python `ipaddress.IPv4Address`
// and `ipaddress.IPv6Address` types.
static IP_COLUMNS: &[ColumnType<'static>] = &[ColumnType::Native(NativeType::Inet)];

// List of CQL column types used to provide clear error messages
// indicating which CQL types are compatible with Python `datetime.time` type.
static TIME_COLUMNS: &[ColumnType<'static>] = &[ColumnType::Native(NativeType::Time)];

// List of CQL column types used to provide clear error messages
// indicating which CQL types are compatible with Python `uuid.UUID` type.
static UUID_COLUMNS: &[ColumnType<'static>] = &[
    ColumnType::Native(NativeType::Uuid),
    ColumnType::Native(NativeType::Timeuuid),
];

fn mk_typck_err<T: ?Sized>(
    got: &ColumnType,
    kind: impl Into<BuiltinTypeCheckErrorKind>,
) -> SerializationError {
    SerializationError::new(BuiltinTypeCheckError {
        rust_name: std::any::type_name::<T>(),
        got: got.clone().into_owned(),
        kind: kind.into(),
    })
}

fn mk_ser_err<T: ?Sized>(
    got: &ColumnType,
    kind: impl Into<BuiltinSerializationErrorKind>,
) -> SerializationError {
    SerializationError::new(BuiltinSerializationError {
        rust_name: std::any::type_name::<T>(),
        got: got.clone().into_owned(),
        kind: kind.into(),
    })
}
