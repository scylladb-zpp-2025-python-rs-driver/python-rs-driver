use chrono_04::NaiveDate;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList, PyMapping, PySet, PyTuple};
use pyo3::{Bound, PyAny, PyErr, PyResult};
use scylla::cluster::metadata::{CollectionType, ColumnType, NativeType, UserDefinedType};
use scylla::serialize::SerializationError;
use scylla::serialize::value::{
    BuiltinSerializationError, BuiltinSerializationErrorKind, BuiltinTypeCheckError,
    BuiltinTypeCheckErrorKind, MapSerializationErrorKind, MapTypeCheckErrorKind,
    SetOrListSerializationErrorKind, SetOrListTypeCheckErrorKind,
};
use scylla::serialize::value::{SerializeValue, UdtTypeCheckErrorKind};
use scylla::serialize::writers::{CellWriter, WrittenCellProof};
use scylla::value::{
    Counter, CqlDate, CqlDecimal, CqlDuration, CqlTime, CqlTimestamp, CqlTimeuuid, CqlValue,
    CqlVarint,
};
use scylla_cql::serialize::value::{
    VectorSerializationErrorKind, serialize_next_variable_length_elem,
};
use scylla_cql::serialize::writers::CellValueBuilder;
use scylla_cql::value::{CqlVarintBorrowed, ValueOverflow};
use std::cmp::max;
use std::net::IpAddr;
use std::ops::Deref;
use std::sync::Arc;
use thiserror::Error;
use time_03::{OffsetDateTime, Time};
use uuid::Uuid;

#[derive(Error, Debug)]
pub(crate) enum PythonDriverSerializationError {
    #[error(transparent)]
    PythonError(#[from] PyErr),

    #[error("Unknown native type")]
    UnknownNativeType,

    #[error("Unknown collection type")]
    UnknownCollectionType,

    #[error("Unknown column type")]
    UnknownColumnType,
}

static INT_COLUMNS: &[ColumnType<'static>] = &[
    ColumnType::Native(NativeType::TinyInt),
    ColumnType::Native(NativeType::SmallInt),
    ColumnType::Native(NativeType::Int),
    ColumnType::Native(NativeType::BigInt),
    ColumnType::Native(NativeType::Counter),
    ColumnType::Native(NativeType::Varint),
];

static FLOAT_COLUMNS: &[ColumnType<'static>] = &[
    ColumnType::Native(NativeType::Double),
    ColumnType::Native(NativeType::Float),
];

static DECIMAL_COLUMNS: &[ColumnType<'static>] = &[ColumnType::Native(NativeType::Decimal)];

static STRING_COLUMNS: &[ColumnType<'static>] = &[
    ColumnType::Native(NativeType::Ascii),
    ColumnType::Native(NativeType::Text),
];

static BOOL_COLUMNS: &[ColumnType<'static>] = &[ColumnType::Native(NativeType::Boolean)];

static BYTES_COLUMNS: &[ColumnType<'static>] = &[ColumnType::Native(NativeType::Blob)];

static RELATIVEDELTA_COLUMNS: &[ColumnType<'static>] = &[ColumnType::Native(NativeType::Duration)];

static DATETIME_COLUMNS: &[ColumnType<'static>] = &[
    ColumnType::Native(NativeType::Date),
    ColumnType::Native(NativeType::Timestamp),
];

static IP_COLUMNS: &[ColumnType<'static>] = &[ColumnType::Native(NativeType::Inet)];

static TIME_COLUMNS: &[ColumnType<'static>] = &[ColumnType::Native(NativeType::Time)];

static UUID_COLUMNS: &[ColumnType<'static>] = &[
    ColumnType::Native(NativeType::Uuid),
    ColumnType::Native(NativeType::Timeuuid),
];

#[derive(Debug)]
pub(crate) struct PyAnyWrapper<'a, 'py>(pub(crate) &'a Bound<'py, PyAny>);

impl<'a, 'py> Deref for PyAnyWrapper<'a, 'py> {
    type Target = &'a Bound<'py, PyAny>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<'a, 'py> PyAnyWrapper<'a, 'py> {
    fn map_type_to_expected(&self) -> Result<&'static [ColumnType<'static>], SerializationError> {
        let python_type_name = self
            .get_type()
            .name()
            .map_err(|e| SerializationError::new(PythonDriverSerializationError::PythonError(e)))?;

        let python_type_name = python_type_name
            .extract::<&str>()
            .map_err(|e| SerializationError::new(PythonDriverSerializationError::PythonError(e)))?;

        let columns = match python_type_name {
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
                    PythonDriverSerializationError::UnknownColumnType,
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

    fn serialize_natives<'b>(
        &self,
        typ: &ColumnType,
        native_type: &NativeType,
        cell_writer: CellWriter<'b>,
    ) -> Result<WrittenCellProof<'b>, SerializationError> {
        match native_type {
            // Integer types.
            NativeType::TinyInt => self.serialize_native::<i8>(typ, cell_writer),
            NativeType::SmallInt => self.serialize_native::<i16>(typ, cell_writer),
            NativeType::Int => self.serialize_native::<i32>(typ, cell_writer),
            NativeType::BigInt => self.serialize_native::<i64>(typ, cell_writer),
            NativeType::Counter => {
                let value = self
                    .extract::<i64>()
                    .map_err(|_| self.mismatched_type_error::<Counter>(typ))?;

                let counter = Counter(value);
                counter.serialize(typ, cell_writer)
            }
            NativeType::Varint => {
                let bits = self
                    .call_method0("bit_length")
                    .and_then(|x| x.extract::<usize>())
                    .map_err(|_| self.mismatched_type_error::<CqlVarint>(typ))?;

                let len = max(1, (bits + 8) / 8);

                let py_dict = PyDict::new(self.py());
                py_dict.set_item("signed", true).unwrap();

                let bytes = self
                    .call_method("to_bytes", (len, "big"), Some(&py_dict))
                    .map_err(|_| self.mismatched_type_error::<CqlVarint>(typ))?;

                let bytes = bytes
                    .downcast::<PyBytes>()
                    .map_err(|_| self.mismatched_type_error::<CqlVarint>(typ))?;

                let bytes = bytes.as_bytes();
                let varint = CqlVarintBorrowed::from_signed_bytes_be_slice(bytes);
                varint.serialize(typ, cell_writer)
            }

            // Float types.
            NativeType::Float => self.serialize_native::<f32>(typ, cell_writer),
            NativeType::Double => self.serialize_native::<f64>(typ, cell_writer),
            NativeType::Decimal => {
                let value = self
                    .extract::<bigdecimal_04::BigDecimal>()
                    .map_err(|_| self.mismatched_type_error::<CqlDecimal>(typ))?;

                let decimal: CqlDecimal = CqlDecimal::try_from(value)
                    .map_err(|_| SerializationError::new(ValueOverflow))?;
                decimal.serialize(typ, cell_writer)
            }

            // Boolean type.
            NativeType::Boolean => self.serialize_native::<bool>(typ, cell_writer),

            // Text types.
            NativeType::Ascii => self.serialize_text(typ, cell_writer),
            NativeType::Text => self.serialize_text(typ, cell_writer),

            // Binary data type.
            NativeType::Blob => {
                let value = self
                    .downcast::<PyBytes>()
                    .map_err(|_| self.mismatched_type_error::<PyBytes>(typ))?;

                let bytes = value.as_bytes();

                bytes.serialize(typ, cell_writer)
            }

            // Datatime types.
            NativeType::Date => {
                let value = self
                    .extract::<NaiveDate>()
                    .map_err(|_| self.mismatched_type_error::<CqlDate>(typ))?;

                let date = CqlDate::from(value);
                date.serialize(typ, cell_writer)
            }
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
                    .extract::<Time>()
                    .map_err(|_| self.mismatched_type_error::<CqlTime>(typ))?;

                let duration = value.duration_since(Time::MIDNIGHT);
                let nanoseconds: i64 = i64::try_from(duration.whole_nanoseconds())
                    .map_err(|_| SerializationError::new(ValueOverflow))?;

                let time = CqlTime(nanoseconds);
                time.serialize(typ, cell_writer)
            }
            NativeType::Timestamp => {
                let value = self
                    .extract::<OffsetDateTime>()
                    .map_err(|_| self.mismatched_type_error::<CqlTimestamp>(typ))?;

                let milliseconds = value.unix_timestamp_nanos() / 1_000_000;

                let milliseconds = i64::try_from(milliseconds)
                    .map_err(|_| SerializationError::new(ValueOverflow))?;

                let timestamp = CqlTimestamp(milliseconds);
                timestamp.serialize(typ, cell_writer)
            }

            // IP address type.
            NativeType::Inet => {
                let value = self
                    .getattr("packed")
                    .map_err(|_| self.mismatched_type_error::<IpAddr>(typ))?;

                let value = value
                    .downcast::<PyBytes>()
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
                let value = value
                    .downcast::<PyBytes>()
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

                let value = value
                    .downcast::<PyBytes>()
                    .map_err(|_| self.mismatched_type_error::<Uuid>(typ))?;

                let bytes = value
                    .extract::<[u8; 16]>()
                    .map_err(|_| self.mismatched_type_error::<Uuid>(typ))?;

                let uuid = Uuid::from_bytes(bytes);
                uuid.serialize(typ, cell_writer)
            }

            _ => Err(SerializationError::new(
                PythonDriverSerializationError::UnknownNativeType,
            )),
        }
    }

    // In future PyO3 release FromPyObjectBound can be used instead of FromPyObject.
    fn serialize_native<'b, T>(
        &self,
        typ: &ColumnType,
        cell_writer: CellWriter<'b>,
    ) -> Result<WrittenCellProof<'b>, SerializationError>
    where
        T: for<'c> pyo3::FromPyObject<'c> + SerializeValue,
    {
        let value = match self.extract::<T>() {
            Ok(val) => val,
            Err(_) => {
                let expected = self.map_type_to_expected()?;
                return Err(SerializationError::new(mk_typck_err::<T>(
                    typ,
                    BuiltinTypeCheckErrorKind::MismatchedType { expected },
                )));
            }
        };

        value.serialize(typ, cell_writer)
    }

    fn serialize_text<'b>(
        &self,
        typ: &ColumnType,
        cell_writer: CellWriter<'b>,
    ) -> Result<WrittenCellProof<'b>, SerializationError> {
        let value = match self.extract::<&str>() {
            Ok(val) => val,
            Err(_) => {
                let expected = self.map_type_to_expected()?;
                return Err(SerializationError::new(mk_typck_err::<&str>(
                    typ,
                    BuiltinTypeCheckErrorKind::MismatchedType { expected },
                )));
            }
        };
        value.serialize(typ, cell_writer)
    }

    fn serialize_types<'b>(
        &self,
        typ: &ColumnType,
        cell_writer: CellWriter<'b>,
    ) -> Result<WrittenCellProof<'b>, SerializationError> {
        match typ {
            ColumnType::Native(native_type) => {
                self.serialize_natives(typ, native_type, cell_writer)
            }

            ColumnType::Collection {
                frozen: _,
                typ: collection_typ,
            } => match collection_typ {
                CollectionType::List(_) => {
                    let list = match PyListWrapper::new(self) {
                        Ok(list) => list,
                        Err(_) => {
                            let expected = self.map_type_to_expected()?;
                            return Err(SerializationError::new(mk_typck_err::<PyList>(
                                typ,
                                BuiltinTypeCheckErrorKind::MismatchedType { expected },
                            )));
                        }
                    };
                    list.serialize(typ, cell_writer)
                }

                CollectionType::Map(_, _) => {
                    let map = match PyMappingWrapper::new(self) {
                        Ok(map) => map,
                        Err(_) => {
                            let expected = self.map_type_to_expected()?;
                            return Err(SerializationError::new(mk_typck_err::<PyMapping>(
                                typ,
                                BuiltinTypeCheckErrorKind::MismatchedType { expected },
                            )));
                        }
                    };
                    map.serialize(typ, cell_writer)
                }

                CollectionType::Set(_) => {
                    let set = match PySetWrapper::new(self) {
                        Ok(set) => set,
                        Err(_) => {
                            let expected = self.map_type_to_expected()?;
                            return Err(SerializationError::new(mk_typck_err::<PySet>(
                                typ,
                                BuiltinTypeCheckErrorKind::MismatchedType { expected },
                            )));
                        }
                    };
                    set.serialize(typ, cell_writer)
                }

                _ => Err(SerializationError::new(
                    PythonDriverSerializationError::UnknownCollectionType,
                )),
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
                    let expected = self.map_type_to_expected()?;
                    Err(SerializationError::new(mk_typck_err::<Self>(
                        typ,
                        BuiltinTypeCheckErrorKind::MismatchedType { expected },
                    )))
                }
            }

            // Supports UDTs passed as Python dicts.
            // For Python dataclass instances, convert to dict first (e.g., using dataclasses.asdict()).
            ColumnType::UserDefinedType { definition, .. } => {
                let dict = match PyUdtWrapper::new(self, definition) {
                    Ok(dict) => dict,
                    Err(_) => {
                        let expected = self.map_type_to_expected()?;
                        return Err(SerializationError::new(mk_typck_err::<PyDict>(
                            typ,
                            BuiltinTypeCheckErrorKind::MismatchedType { expected },
                        )));
                    }
                };
                dict.serialize(typ, cell_writer)
            }

            ColumnType::Tuple(elements_types) => {
                let tuple = match PyTupleWrapper::new(self, elements_types) {
                    Ok(tuple) => tuple,
                    Err(_) => {
                        let expected = self.map_type_to_expected()?;
                        return Err(SerializationError::new(mk_typck_err::<PyTuple>(
                            typ,
                            BuiltinTypeCheckErrorKind::MismatchedType { expected },
                        )));
                    }
                };
                tuple.serialize(typ, cell_writer)
            }
            _ => Err(SerializationError::new(
                PythonDriverSerializationError::UnknownColumnType,
            )),
        }
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

        self.serialize_types(typ, cell_writer)
    }
}

#[derive(Debug)]
pub(crate) struct PyListWrapper<'a, 'py> {
    pub(crate) inner: &'a Bound<'py, PyList>,
}

impl<'a, 'py> Deref for PyListWrapper<'a, 'py> {
    type Target = Bound<'py, PyList>;

    fn deref(&self) -> &Self::Target {
        self.inner
    }
}

impl<'a, 'py> PyListWrapper<'a, 'py> {
    fn new(value: &PyAnyWrapper<'a, 'py>) -> PyResult<Self> {
        let list: &Bound<PyList> = value.downcast::<PyList>()?;
        Ok(PyListWrapper {
            inner: list,
            // inner: Bound::clone(list),
        })
    }
}

impl<'a, 'py> SerializeValue for PyListWrapper<'a, 'py> {
    fn serialize<'b>(
        &self,
        typ: &ColumnType,
        cell_writer: CellWriter<'b>,
    ) -> Result<WrittenCellProof<'b>, SerializationError> {
        let items = self.iter();

        serialize_sequence(
            std::any::type_name::<PyList>(),
            items.len(),
            items,
            typ,
            cell_writer,
        )
    }
}

pub(crate) struct PySetWrapper<'py, 'a>(&'a Bound<'py, PySet>);

impl<'a, 'py> Deref for PySetWrapper<'a, 'py> {
    type Target = Bound<'py, PySet>;

    fn deref(&self) -> &Self::Target {
        self.0
    }
}

impl<'a, 'py> PySetWrapper<'a, 'py> {
    fn new(value: &PyAnyWrapper<'a, 'py>) -> PyResult<Self> {
        let set: &Bound<PySet> = value.downcast::<PySet>()?;
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

        serialize_sequence(
            std::any::type_name::<PySet>(),
            items.len(),
            items,
            typ,
            cell_writer,
        )
    }
}

fn serialize_sequence<'t, 'b, 'py>(
    rust_name: &'static str,
    len: usize,
    iter: impl Iterator<Item = Bound<'py, PyAny>>,
    typ: &ColumnType,
    writer: CellWriter<'b>,
) -> Result<WrittenCellProof<'b>, SerializationError> {
    let elt = match typ {
        ColumnType::Collection {
            typ: CollectionType::List(elt),
            ..
        }
        | ColumnType::Collection {
            typ: CollectionType::Set(elt),
            ..
        } => elt,
        _ => {
            return Err(mk_typck_err_named(
                rust_name,
                typ,
                SetOrListTypeCheckErrorKind::NotSetOrList,
            ));
        }
    };

    let mut builder = writer.into_value_builder();

    let element_count: i32 = len.try_into().map_err(|_| {
        mk_ser_err_named(
            rust_name,
            typ,
            SetOrListSerializationErrorKind::TooManyElements,
        )
    })?;
    builder.append_bytes(&element_count.to_be_bytes());

    for el in iter {
        PyAnyWrapper::serialize(&PyAnyWrapper(&el), elt, builder.make_sub_writer()).map_err(
            |err| {
                mk_ser_err_named(
                    rust_name,
                    typ,
                    SetOrListSerializationErrorKind::ElementSerializationFailed(err),
                )
            },
        )?;
    }

    builder
        .finish()
        .map_err(|_| mk_ser_err_named(rust_name, typ, BuiltinSerializationErrorKind::SizeOverflow))
}

pub(crate) struct PyMappingWrapper<'a, 'py>(&'a Bound<'py, PyMapping>);

impl<'a, 'py> Deref for PyMappingWrapper<'a, 'py> {
    type Target = Bound<'py, PyMapping>;

    fn deref(&self) -> &Self::Target {
        self.0
    }
}

impl<'a, 'py> PyMappingWrapper<'a, 'py> {
    fn new(value: &PyAnyWrapper<'a, 'py>) -> PyResult<Self> {
        let map: &Bound<PyMapping> = value.downcast::<PyMapping>()?;
        Ok(PyMappingWrapper(map))
    }
}

impl<'a, 'py> SerializeValue for PyMappingWrapper<'a, 'py> {
    fn serialize<'b>(
        &self,
        typ: &ColumnType,
        cell_writer: CellWriter<'b>,
    ) -> Result<WrittenCellProof<'b>, SerializationError> {
        let (ktyp, vtyp) = match typ {
            ColumnType::Collection {
                frozen: false,
                typ: CollectionType::Map(k, v),
            } => (k, v),
            _ => {
                return Err(mk_typck_err::<PyMapping>(
                    typ,
                    MapTypeCheckErrorKind::NotMap,
                ));
            }
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
            PyAnyWrapper::serialize(&PyAnyWrapper(&key), ktyp, builder.make_sub_writer()).map_err(
                |err| {
                    mk_ser_err::<PyMapping>(
                        typ,
                        MapSerializationErrorKind::KeySerializationFailed(err),
                    )
                },
            )?;
            PyAnyWrapper::serialize(&PyAnyWrapper(&value), vtyp, builder.make_sub_writer())
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
pub(crate) struct PyListVectorWrapper<'py, 'a> {
    pub(crate) inner: &'a Bound<'py, PyList>,
    pub(crate) dimension: u16,
    pub(crate) element_type: &'a ColumnType<'a>,
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
        let list: &Bound<PyList> = value.downcast::<PyList>()?;
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
        // let items: Vec<PyAnyWrapper> = self.iter().map(PyAnyWrapper).collect::<Vec<_>>();
        let items = self.iter();

        serialize_vector(
            std::any::type_name::<PyList>(),
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
pub(crate) struct PyTupleVectorWrapper<'py, 'a> {
    pub(crate) inner: Bound<'py, PyTuple>,
    pub(crate) dimension: u16,
    pub(crate) element_type: &'a ColumnType<'a>,
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
        let tuple: &Bound<PyTuple> = value.downcast::<PyTuple>()?;

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
        // let items: Vec<PyAnyWrapper> = self.iter().map(PyAnyWrapper).collect::<Vec<_>>();
        let items = self.iter();

        serialize_vector(
            std::any::type_name::<PyTuple>(),
            items.len(),
            items,
            self.element_type,
            self.dimension,
            typ,
            cell_writer,
        )
    }
}

pub fn serialize_vector<'t, 'b, 'py>(
    rust_name: &'static str,
    len: usize,
    iter: impl Iterator<Item = Bound<'py, PyAny>>,
    element_type: &ColumnType,
    dimensions: u16,
    typ: &ColumnType,
    writer: CellWriter<'b>,
) -> Result<WrittenCellProof<'b>, SerializationError> {
    if len != dimensions as usize {
        return Err(mk_ser_err_named(
            rust_name,
            typ,
            VectorSerializationErrorKind::InvalidNumberOfElements(len, dimensions),
        ));
    }
    let mut builder = writer.into_value_builder();
    match type_size(element_type) {
        Some(_) => {
            for element in iter {
                serialize_next_constant_length_elem(
                    rust_name,
                    element_type,
                    typ,
                    &mut builder,
                    &PyAnyWrapper(&element),
                )?;
            }
        }
        None => {
            for element in iter {
                serialize_next_variable_length_elem(
                    rust_name,
                    element_type,
                    typ,
                    &mut builder,
                    &PyAnyWrapper(&element),
                )?;
            }
        }
    }

    builder
        .finish()
        .map_err(|_| mk_ser_err_named(rust_name, typ, BuiltinSerializationErrorKind::SizeOverflow))
}

fn type_size(typ: &ColumnType) -> Option<usize> {
    match typ {
        ColumnType::Native(n) => native_type_size(n),
        ColumnType::Tuple(_) => None,
        ColumnType::Collection { .. } => None,
        ColumnType::Vector { typ, dimensions } => {
            type_size(typ).map(|size| size * usize::from(*dimensions))
        }
        ColumnType::UserDefinedType { .. } => None,
        _ => None,
    }
}

fn native_type_size(typ: &NativeType) -> Option<usize> {
    match typ {
        NativeType::Ascii => None,
        NativeType::Boolean => Some(1),
        NativeType::Blob => None,
        NativeType::Counter => None,
        NativeType::Date => None,
        NativeType::Decimal => None,
        NativeType::Double => Some(8),
        NativeType::Duration => None,
        NativeType::Float => Some(4),
        NativeType::Int => Some(4),
        NativeType::BigInt => Some(8),
        NativeType::Text => None,
        NativeType::Timestamp => Some(8),
        NativeType::Inet => None,
        NativeType::SmallInt => None,
        NativeType::TinyInt => None,
        NativeType::Time => None,
        NativeType::Timeuuid => Some(16),
        NativeType::Uuid => Some(16),
        NativeType::Varint => None,
        _ => None,
    }
}

fn serialize_next_constant_length_elem<'t, T: SerializeValue + 't>(
    rust_name: &'static str,
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
        mk_ser_err_named(
            rust_name,
            typ,
            VectorSerializationErrorKind::ElementSerializationFailed(err),
        )
    })?;
    Ok(())
}

pub(crate) struct PyUdtWrapper<'py, 'a> {
    pub(crate) inner: &'a Bound<'py, PyDict>,
    pub(crate) definition: &'a Arc<UserDefinedType<'a>>,
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
        let dict: &Bound<PyDict> = value.downcast::<PyDict>()?;
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

            PyAnyWrapper(&item).serialize_types(field_type, builder.make_sub_writer())?;
        }

        builder
            .finish()
            .map_err(|_| mk_ser_err::<CqlValue>(typ, BuiltinSerializationErrorKind::SizeOverflow))
    }
}

#[derive(Debug)]
pub(crate) struct PyTupleWrapper<'py, 'a> {
    pub(crate) inner: &'a Bound<'py, PyTuple>,
    pub(crate) elements_types: &'a Vec<ColumnType<'a>>,
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
        let tuple: &Bound<PyTuple> = value.downcast::<PyTuple>()?;
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
            PyAnyWrapper(&val).serialize_types(element_type, builder.make_sub_writer())?;
        }

        builder
            .finish()
            .map_err(|_| mk_ser_err::<CqlValue>(typ, BuiltinSerializationErrorKind::SizeOverflow))
    }
}

fn mk_typck_err<T: ?Sized>(
    got: &ColumnType,
    kind: impl Into<BuiltinTypeCheckErrorKind>,
) -> SerializationError {
    mk_typck_err_named(std::any::type_name::<T>(), got, kind)
}

pub(crate) fn mk_typck_err_named(
    name: &'static str,
    got: &ColumnType,
    kind: impl Into<BuiltinTypeCheckErrorKind>,
) -> SerializationError {
    SerializationError::new(BuiltinTypeCheckError {
        rust_name: name,
        got: got.clone().into_owned(),
        kind: kind.into(),
    })
}

pub(crate) fn mk_ser_err<T: ?Sized>(
    got: &ColumnType,
    kind: impl Into<BuiltinSerializationErrorKind>,
) -> SerializationError {
    mk_ser_err_named(std::any::type_name::<T>(), got, kind)
}

pub(crate) fn mk_ser_err_named(
    name: &'static str,
    got: &ColumnType,
    kind: impl Into<BuiltinSerializationErrorKind>,
) -> SerializationError {
    SerializationError::new(BuiltinSerializationError {
        rust_name: name,
        got: got.clone().into_owned(),
        kind: kind.into(),
    })
}
