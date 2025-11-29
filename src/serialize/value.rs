use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyList, PyMapping, PySet, PyTuple};
use pyo3::{Bound, PyAny, PyErr, PyResult};
use scylla_cql::_macro_internal::{
    CellWriter, ColumnType, SerializationError, SerializeValue, UdtTypeCheckErrorKind,
    WrittenCellProof,
};
use scylla_cql::frame::response::result::{CollectionType, NativeType, UserDefinedType};
use scylla_cql::serialize::value::{
    BuiltinSerializationError, BuiltinSerializationErrorKind, BuiltinTypeCheckError,
    BuiltinTypeCheckErrorKind, MapSerializationErrorKind, MapTypeCheckErrorKind,
    SetOrListSerializationErrorKind, SetOrListTypeCheckErrorKind, serialize_vector,
};
use scylla_cql::value::{
    Counter, CqlDate, CqlDecimal, CqlDuration, CqlTime, CqlTimestamp, CqlTimeuuid, CqlValue,
    CqlVarint,
};
use std::net::IpAddr;
use std::ops::Deref;
use std::sync::Arc;
use thiserror::Error;
use uuid::Uuid;

#[derive(Error, Debug)]
pub(crate) enum PythonDriverSerializationError {
    #[error(transparent)]
    PythonError(#[from] PyErr),

    #[error("Unknown native type")]
    UnknownNativeType,

    #[error("Unknown collection type")]
    UnknownCollectionType,

    #[error("Unknown collection type")]
    UnknownColumnType,
}

static INT_COLUMNS: &[ColumnType<'static>] = &[
    ColumnType::Native(NativeType::Int),
    ColumnType::Native(NativeType::BigInt),
    ColumnType::Native(NativeType::Date),
    ColumnType::Native(NativeType::Timestamp),
    ColumnType::Native(NativeType::Time),
    ColumnType::Native(NativeType::SmallInt),
    ColumnType::Native(NativeType::TinyInt),
];

static FLOAT_COLUMNS: &[ColumnType<'static>] = &[
    ColumnType::Native(NativeType::Double),
    ColumnType::Native(NativeType::Float),
];

static STRING_COLUMNS: &[ColumnType<'static>] = &[
    ColumnType::Native(NativeType::Ascii),
    ColumnType::Native(NativeType::Text),
];

static BOOL_COLUMNS: &[ColumnType<'static>] = &[ColumnType::Native(NativeType::Boolean)];

#[derive(Debug)]
pub(crate) struct PyAnyWrapper<'py>(pub(crate) Bound<'py, PyAny>);

impl<'py> Deref for PyAnyWrapper<'py> {
    type Target = Bound<'py, PyAny>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<'py> PyAnyWrapper<'py> {
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

            // Only a subset of Python types is currently supported. More can be added after
            // deciding on error handling approach.
            _ => unimplemented!("{:?}", python_type_name),
        };

        Ok(columns)
    }

    fn mismatched_type_error(&self, name: &'static str, typ: &ColumnType) -> SerializationError {
        let expected = match self.map_type_to_expected() {
            Ok(expected) => expected,
            Err(e) => return e,
        };
        SerializationError::new(mk_typck_err_named(
            name,
            typ,
            BuiltinTypeCheckErrorKind::MismatchedType { expected },
        ))
    }

    fn serialize_natives<'a>(
        &self,
        typ: &ColumnType,
        native_type: &NativeType,
        cell_writer: CellWriter<'a>,
    ) -> Result<WrittenCellProof<'a>, SerializationError> {
        match native_type {
            NativeType::Ascii => self.serialize_text(typ, cell_writer),
            NativeType::Boolean => self.serialize_native::<bool>(typ, cell_writer),
            NativeType::Blob => self.serialize_native::<Vec<u8>>(typ, cell_writer),
            NativeType::Counter => {
                let value = self.extract::<i64>().map_err(|_| {
                    self.mismatched_type_error(std::any::type_name::<Counter>(), typ)
                })?;

                let counter = Counter(value);
                counter.serialize(typ, cell_writer)
            }
            NativeType::Date => {
                let value = self.extract::<i64>().map_err(|_| {
                    self.mismatched_type_error(std::any::type_name::<CqlDate>(), typ)
                })?;

                let value: u32 = u32::try_from(value).map_err(|_| {
                    self.mismatched_type_error(std::any::type_name::<CqlDate>(), typ)
                })?;

                let date = CqlDate(value);
                date.serialize(typ, cell_writer)
            }
            NativeType::Decimal => {
                let value = self.extract::<(Vec<u8>, i32)>().map_err(|_| {
                    self.mismatched_type_error(std::any::type_name::<CqlDecimal>(), typ)
                })?;

                let decimal = CqlDecimal::from_signed_be_bytes_and_exponent(value.0, value.1);
                decimal.serialize(typ, cell_writer)
            }
            NativeType::Double => self.serialize_native::<f64>(typ, cell_writer),
            NativeType::Duration => {
                let value = self.extract::<(i32, i32, i64)>().map_err(|_| {
                    self.mismatched_type_error(std::any::type_name::<CqlDuration>(), typ)
                })?;

                let duration = CqlDuration {
                    months: value.0,
                    days: value.1,
                    nanoseconds: value.2,
                };
                duration.serialize(typ, cell_writer)
            }
            NativeType::Float => self.serialize_native::<f32>(typ, cell_writer),
            NativeType::Int => self.serialize_native::<i32>(typ, cell_writer),
            NativeType::BigInt => self.serialize_native::<i64>(typ, cell_writer),
            NativeType::Text => self.serialize_text(typ, cell_writer),
            NativeType::Timestamp => {
                let value = self.extract::<i64>().map_err(|_| {
                    self.mismatched_type_error(std::any::type_name::<CqlTimestamp>(), typ)
                })?;

                let timestamp = CqlTimestamp(value);
                timestamp.serialize(typ, cell_writer)
            }
            NativeType::Inet => {
                let value = self.getattr("packed").map_err(|_| {
                    self.mismatched_type_error(std::any::type_name::<IpAddr>(), typ)
                })?;

                let value = value.downcast::<PyBytes>().map_err(|_| {
                    self.mismatched_type_error(std::any::type_name::<IpAddr>(), typ)
                })?;

                let bytes = value.as_bytes();

                let ip = match bytes.len() {
                    4 => {
                        let val: [u8; 4] = bytes.try_into().map_err(|_| {
                            self.mismatched_type_error(std::any::type_name::<IpAddr>(), typ)
                        })?;
                        IpAddr::from(val)
                    }
                    16 => {
                        let val: [u8; 16] = bytes.try_into().map_err(|_| {
                            self.mismatched_type_error(std::any::type_name::<IpAddr>(), typ)
                        })?;
                        IpAddr::from(val)
                    }
                    _ => {
                        return Err(
                            self.mismatched_type_error(std::any::type_name::<IpAddr>(), typ)
                        );
                    }
                };

                ip.serialize(typ, cell_writer)
            }
            NativeType::SmallInt => self.serialize_native::<i16>(typ, cell_writer),
            NativeType::TinyInt => self.serialize_native::<i8>(typ, cell_writer),
            NativeType::Time => {
                let value = self.extract::<i64>().map_err(|_| {
                    self.mismatched_type_error(std::any::type_name::<CqlTime>(), typ)
                })?;

                let time = CqlTime(value);
                time.serialize(typ, cell_writer)
            }
            NativeType::Timeuuid => {
                let value = self.getattr("bytes").map_err(|_| {
                    self.mismatched_type_error(std::any::type_name::<CqlTimeuuid>(), typ)
                })?;

                let value = value.downcast::<PyBytes>().map_err(|_| {
                    self.mismatched_type_error(std::any::type_name::<CqlTimeuuid>(), typ)
                })?;

                let bytes = value.extract::<[u8; 16]>().map_err(|_| {
                    self.mismatched_type_error(std::any::type_name::<CqlTimeuuid>(), typ)
                })?;

                let timeuuid = CqlTimeuuid::from_bytes(bytes);
                timeuuid.serialize(typ, cell_writer)
            }
            NativeType::Uuid => {
                let value = self
                    .getattr("bytes")
                    .map_err(|_| self.mismatched_type_error(std::any::type_name::<Uuid>(), typ))?;

                let value = value
                    .downcast::<PyBytes>()
                    .map_err(|_| self.mismatched_type_error(std::any::type_name::<Uuid>(), typ))?;

                let bytes = value
                    .extract::<[u8; 16]>()
                    .map_err(|_| self.mismatched_type_error(std::any::type_name::<Uuid>(), typ))?;

                let uuid = Uuid::from_bytes(bytes);
                uuid.serialize(typ, cell_writer)
            }
            NativeType::Varint => {
                let value = self.extract::<Vec<u8>>().map_err(|_| {
                    self.mismatched_type_error(std::any::type_name::<CqlVarint>(), typ)
                })?;

                let variant = CqlVarint::from_signed_bytes_be(value);
                variant.serialize(typ, cell_writer)
            }
            _ => Err(SerializationError::new(
                PythonDriverSerializationError::UnknownNativeType,
            )),
        }
    }

    // In future PyO3 release FromPyObjectBound can be used instead of FromPyObject.
    fn serialize_native<'a, T>(
        &self,
        typ: &ColumnType,
        cell_writer: CellWriter<'a>,
    ) -> Result<WrittenCellProof<'a>, SerializationError>
    where
        T: for<'b> pyo3::FromPyObject<'b> + SerializeValue,
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

    fn serialize_text<'a>(
        &self,
        typ: &ColumnType,
        cell_writer: CellWriter<'a>,
    ) -> Result<WrittenCellProof<'a>, SerializationError> {
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

    fn serialize_types<'a>(
        &self,
        typ: &ColumnType,
        cell_writer: CellWriter<'a>,
    ) -> Result<WrittenCellProof<'a>, SerializationError> {
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

impl<'py> SerializeValue for PyAnyWrapper<'py> {
    fn serialize<'a>(
        &self,
        typ: &ColumnType,
        cell_writer: CellWriter<'a>,
    ) -> Result<WrittenCellProof<'a>, SerializationError> {
        if self.is_none() {
            return Ok(cell_writer.set_null());
        }

        self.serialize_types(typ, cell_writer)
    }
}

#[derive(Debug)]
pub(crate) struct PyListWrapper<'py> {
    pub(crate) inner: Bound<'py, PyList>,
}

impl<'py> Deref for PyListWrapper<'py> {
    type Target = Bound<'py, PyList>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<'py> PyListWrapper<'py> {
    fn new(value: &PyAnyWrapper<'py>) -> PyResult<Self> {
        let list: &Bound<PyList> = value.downcast::<PyList>()?;
        Ok(PyListWrapper {
            inner: Bound::clone(list),
        })
    }
}

impl<'py> SerializeValue for PyListWrapper<'py> {
    fn serialize<'b>(
        &self,
        typ: &ColumnType,
        cell_writer: CellWriter<'b>,
    ) -> Result<WrittenCellProof<'b>, SerializationError> {
        let items: Vec<PyAnyWrapper> = self.iter().map(PyAnyWrapper).collect::<Vec<_>>();

        serialize_sequence(
            std::any::type_name::<PyList>(),
            items.len(),
            items.iter(),
            typ,
            cell_writer,
        )
    }
}

pub(crate) struct PyMappingWrapper<'py>(Bound<'py, PyMapping>);

impl<'py> Deref for PyMappingWrapper<'py> {
    type Target = Bound<'py, PyMapping>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<'py> PyMappingWrapper<'py> {
    fn new(value: &PyAnyWrapper<'py>) -> PyResult<Self> {
        let map: &Bound<PyMapping> = value.downcast::<PyMapping>()?;
        Ok(PyMappingWrapper(Bound::clone(map)))
    }
}

impl<'py> SerializeValue for PyMappingWrapper<'py> {
    fn serialize<'b>(
        &self,
        typ: &ColumnType,
        cell_writer: CellWriter<'b>,
    ) -> Result<WrittenCellProof<'b>, SerializationError> {
        let mut keys: Vec<PyAnyWrapper<'py>> = Vec::new();
        let mut values: Vec<PyAnyWrapper<'py>> = Vec::new();

        let iter_items = self
            .items()
            .map_err(|e| SerializationError::new(PythonDriverSerializationError::PythonError(e)))?;

        for pair in iter_items {
            let (key, value) = pair
                .extract::<(Bound<'py, PyAny>, Bound<'py, PyAny>)>()
                .unwrap();

            keys.push(PyAnyWrapper(key));
            values.push(PyAnyWrapper(value));
        }

        serialize_mapping(
            std::any::type_name::<PyMapping>(),
            keys.len(),
            keys.iter().zip(values.iter()),
            typ,
            cell_writer,
        )
    }
}

fn serialize_mapping<'t, 'b, K: SerializeValue + 't, V: SerializeValue + 't>(
    rust_name: &'static str,
    len: usize,
    iter: impl Iterator<Item = (&'t K, &'t V)>,
    typ: &ColumnType,
    writer: CellWriter<'b>,
) -> Result<WrittenCellProof<'b>, SerializationError> {
    let (ktyp, vtyp) = match typ {
        ColumnType::Collection {
            frozen: false,
            typ: CollectionType::Map(k, v),
        } => (k, v),
        _ => {
            return Err(mk_typck_err_named(
                rust_name,
                typ,
                MapTypeCheckErrorKind::NotMap,
            ));
        }
    };

    let mut builder = writer.into_value_builder();

    let element_count: i32 = len.try_into().map_err(|_| {
        mk_ser_err_named(rust_name, typ, MapSerializationErrorKind::TooManyElements)
    })?;
    builder.append_bytes(&element_count.to_be_bytes());

    for (k, v) in iter {
        K::serialize(k, ktyp, builder.make_sub_writer()).map_err(|err| {
            mk_ser_err_named(
                rust_name,
                typ,
                MapSerializationErrorKind::KeySerializationFailed(err),
            )
        })?;
        V::serialize(v, vtyp, builder.make_sub_writer()).map_err(|err| {
            mk_ser_err_named(
                rust_name,
                typ,
                MapSerializationErrorKind::ValueSerializationFailed(err),
            )
        })?;
    }

    builder
        .finish()
        .map_err(|_| mk_ser_err_named(rust_name, typ, BuiltinSerializationErrorKind::SizeOverflow))
}

pub(crate) struct PySetWrapper<'py>(Bound<'py, PySet>);

impl<'py> Deref for PySetWrapper<'py> {
    type Target = Bound<'py, PySet>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<'py> PySetWrapper<'py> {
    fn new(value: &PyAnyWrapper<'py>) -> PyResult<Self> {
        let set: &Bound<PySet> = value.downcast::<PySet>()?;
        Ok(PySetWrapper(Bound::clone(set)))
    }
}

impl<'py> SerializeValue for PySetWrapper<'py> {
    fn serialize<'b>(
        &self,
        typ: &ColumnType,
        cell_writer: CellWriter<'b>,
    ) -> Result<WrittenCellProof<'b>, SerializationError> {
        let items: Vec<PyAnyWrapper> = self.iter().map(PyAnyWrapper).collect::<Vec<_>>();

        serialize_sequence(
            std::any::type_name::<PySet>(),
            items.len(),
            items.iter(),
            typ,
            cell_writer,
        )
    }
}

fn serialize_sequence<'t, 'b, T: SerializeValue + 't>(
    rust_name: &'static str,
    len: usize,
    iter: impl Iterator<Item = &'t T>,
    typ: &ColumnType,
    writer: CellWriter<'b>,
) -> Result<WrittenCellProof<'b>, SerializationError> {
    let elt = match typ {
        ColumnType::Collection {
            frozen: false,
            typ: CollectionType::List(elt),
        }
        | ColumnType::Collection {
            frozen: false,
            typ: CollectionType::Set(elt),
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
        T::serialize(el, elt, builder.make_sub_writer()).map_err(|err| {
            mk_ser_err_named(
                rust_name,
                typ,
                SetOrListSerializationErrorKind::ElementSerializationFailed(err),
            )
        })?;
    }

    builder
        .finish()
        .map_err(|_| mk_ser_err_named(rust_name, typ, BuiltinSerializationErrorKind::SizeOverflow))
}

#[derive(Debug)]
pub(crate) struct PyListVectorWrapper<'py, 'a> {
    pub(crate) inner: Bound<'py, PyList>,
    pub(crate) dimension: u16,
    pub(crate) element_type: &'a ColumnType<'a>,
}

impl<'py, 'a> Deref for PyListVectorWrapper<'py, 'a> {
    type Target = Bound<'py, PyList>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<'py, 'a> PyListVectorWrapper<'py, 'a> {
    fn new(
        value: &PyAnyWrapper<'py>,
        dimension: u16,
        element_type: &'a ColumnType<'a>,
    ) -> PyResult<Self> {
        let list: &Bound<PyList> = value.downcast::<PyList>()?;
        Ok(PyListVectorWrapper {
            inner: Bound::clone(list),
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
        let items: Vec<PyAnyWrapper> = self.iter().map(PyAnyWrapper).collect::<Vec<_>>();

        serialize_vector(
            std::any::type_name::<PyList>(),
            items.len(),
            items.iter(),
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
        value: &PyAnyWrapper<'py>,
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
        let items: Vec<PyAnyWrapper> = self.iter().map(PyAnyWrapper).collect::<Vec<_>>();

        serialize_vector(
            std::any::type_name::<PyList>(),
            items.len(),
            items.iter(),
            self.element_type,
            self.dimension,
            typ,
            cell_writer,
        )
    }
}

pub(crate) struct PyUdtWrapper<'py, 'a> {
    pub(crate) inner: Bound<'py, PyDict>,
    pub(crate) definition: &'a Arc<UserDefinedType<'a>>,
}

impl<'py> Deref for PyUdtWrapper<'py, '_> {
    type Target = Bound<'py, PyDict>;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<'py, 'a> PyUdtWrapper<'py, 'a> {
    fn new(value: &PyAnyWrapper<'py>, definition: &'a Arc<UserDefinedType<'a>>) -> PyResult<Self> {
        let dict: &Bound<PyDict> = value.downcast::<PyDict>()?;
        Ok(PyUdtWrapper {
            inner: dict.clone(),
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

            PyAnyWrapper(item).serialize_types(field_type, builder.make_sub_writer())?;
        }

        builder
            .finish()
            .map_err(|_| mk_ser_err::<CqlValue>(typ, BuiltinSerializationErrorKind::SizeOverflow))
    }
}

#[derive(Debug)]
pub(crate) struct PyTupleWrapper<'py, 'a> {
    pub(crate) inner: Bound<'py, PyTuple>,
    pub(crate) elements_types: &'a Vec<ColumnType<'a>>,
}

impl<'py> Deref for PyTupleWrapper<'py, '_> {
    type Target = Bound<'py, PyTuple>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl<'py, 'a> PyTupleWrapper<'py, 'a> {
    fn new(value: &PyAnyWrapper<'py>, elements_types: &'a Vec<ColumnType<'_>>) -> PyResult<Self> {
        let tuple: &Bound<PyTuple> = value.downcast::<PyTuple>()?;
        Ok(PyTupleWrapper {
            inner: Bound::clone(tuple),
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
            PyAnyWrapper(val).serialize_types(element_type, builder.make_sub_writer())?;
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

pub(crate) fn mk_typck_err_row<T>(
    kind: impl Into<scylla::serialize::row::BuiltinTypeCheckErrorKind>,
) -> SerializationError {
    mk_typck_err_named_row(std::any::type_name::<T>(), kind)
}

pub(crate) fn mk_typck_err_named_row(
    name: &'static str,
    kind: impl Into<scylla::serialize::row::BuiltinTypeCheckErrorKind>,
) -> SerializationError {
    SerializationError::new(scylla::serialize::row::BuiltinTypeCheckError {
        rust_name: name,
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
