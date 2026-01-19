use crate::deserialize::conversion::{CqlDurationWrapper, CqlVarintWrapper};
use crate::errors::DeserializationError as DriverDeserializationError;
use crate::errors::{decode_err, py_conv_err};
use bigdecimal_04::BigDecimal;
use chrono_04::{DateTime, NaiveTime, Utc};
// use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::{PyDictMethods, PyListMethods, PyModule, PyModuleMethods, PySetMethods};
use pyo3::sync::PyOnceLock;
use pyo3::types::{
    PyBool, PyBytes, PyDict, PyFloat, PyInt, PyList, PyNone, PySet, PyString, PyTuple,
};
use pyo3::{Bound, IntoPyObject, Py, PyAny, PyResult, Python, pyclass, pymethods, pymodule};
use scylla_cql::deserialize::value::{
    BuiltinDeserializationErrorKind, FixedLengthBytesSequenceIterator, MapDeserializationErrorKind,
    SetOrListDeserializationErrorKind, mk_deser_err,
};
use scylla_cql::deserialize::value::{DeserializeValue, UdtIterator};
use scylla_cql::deserialize::{DeserializationError, FrameSlice};
use scylla_cql::frame::frame_errors::LowLevelDeserializationError;
use scylla_cql::frame::response::result::ColumnType;
use scylla_cql::frame::response::result::ColumnType::Native;
use scylla_cql::frame::response::result::{CollectionType, NativeType};
use scylla_cql::frame::types;
use scylla_cql::value::{
    Counter, CqlDate, CqlDecimalBorrowed, CqlDuration, CqlTime, CqlTimestamp, CqlTimeuuid,
    CqlVarintBorrowed,
};
use std::convert::Infallible;
use std::marker::PhantomData;
use std::net::IpAddr;
use std::sync::Arc;

// NOTE: I intentionally do NOT use Scylla's `DeserializeValue` trait here.
// The trait does not provide a `Python` argument, meaning that Python objects which
// would have to be constructed inside `deserialize()` or deeper in recursion
// would require acquiring the GIL separately for every column/element/UDT/List
// during nested decoding.
//
// Using our own `PyDeserializeValue` trait keeps all Python conversions inside
// a single `Python::with_gil(...)` boundary, allowing complex values (lists,
// sets, UDTs, nested collections) to be deserialized without repeated GIL
// acquisition. This avoids potential slowdown of acquiring GIL multiples times.
//
// If GIL-per-python object created would not be considered a problem and
// eliminating the need of rewriting DeserializeValue trait would be more beneficial
// We could switch back to using `DeserializeValue` trait.
pub(crate) trait PyDeserializeValue<'frame, 'metadata, 'py>: Sized {
    fn deserialize_py(
        typ: &'metadata ColumnType<'metadata>,
        v: Option<FrameSlice<'frame>>,
        py: Python<'py>,
    ) -> Result<PyDeserializedValue, DriverDeserializationError>;
}

impl PyDeserializedValue {
    fn new(value: Bound<'_, PyAny>) -> Self {
        Self {
            value: value.into(),
        }
    }

    fn none(py: Python) -> Self {
        Self {
            value: PyNone::get(py).to_owned().into(),
        }
    }

    fn empty_value(py: Python<'_>) -> PyResult<Self> {
        static EMPTY_CQL_VALUE: PyOnceLock<Py<CqlEmpty>> = PyOnceLock::new();
        let empty: &Py<CqlEmpty> = EMPTY_CQL_VALUE.get_or_try_init(py, || Py::new(py, CqlEmpty))?;

        Ok(Self {
            value: Py::clone_ref(empty.as_any(), py),
        })
    }
}

impl<'py> IntoPyObject<'py> for PyDeserializedValue {
    type Target = PyAny;
    type Output = Bound<'py, PyAny>;
    type Error = Infallible;
    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        Ok(self.value.into_bound(py))
    }
}

impl<'py> IntoPyObject<'py> for &PyDeserializedValue {
    type Target = PyAny;
    type Output = Bound<'py, PyAny>;
    type Error = Infallible;
    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        Ok(self.value.clone_ref(py).into_bound(py))
    }
}

impl<'frame, 'metadata, 'py> PyDeserializeValue<'frame, 'metadata, 'py> for PyDeserializedValue {
    fn deserialize_py(
        typ: &'metadata ColumnType<'metadata>,
        v: Option<FrameSlice<'frame>>,
        py: Python<'py>,
    ) -> Result<Self, DriverDeserializationError> {
        deser_cql_py_value(py, typ, v)
    }
}

pub(crate) struct PyDeserializedValue {
    value: Py<PyAny>,
}

struct PyValueOrError {
    result: Result<PyDeserializedValue, DriverDeserializationError>,
}

impl PyValueOrError {
    fn new(result: Result<PyDeserializedValue, DriverDeserializationError>) -> Self {
        PyValueOrError { result }
    }
}

impl<'py> IntoPyObject<'py> for PyValueOrError {
    type Target = PyAny;
    type Output = Bound<'py, PyAny>;
    type Error = DriverDeserializationError;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        match self.result {
            Ok(value) => {
                let obj = match value.into_pyobject(py) {
                    Ok(obj) => obj,
                    Err(never) => match never {},
                };
                Ok(obj)
            }
            Err(e) => Err(e),
        }
    }
}

#[pyclass(frozen)]
pub struct CqlEmpty;

#[pymethods]
impl CqlEmpty {
    fn __repr__(&self) -> &'static str {
        "CqlEmpty"
    }
}

struct List<T> {
    phantom_data: PhantomData<T>,
}

fn deserialize_sequence<'frame, 'metadata, 'py, T, FBuild>(
    typ: &'metadata ColumnType<'metadata>,
    mut v: FrameSlice<'frame>,
    py: Python<'py>,
    elem_typ: &'metadata ColumnType<'metadata>,
    mut builder: FBuild,
) -> Result<(), DriverDeserializationError>
where
    T: PyDeserializeValue<'frame, 'metadata, 'py>,
    FBuild: FnMut(PyDeserializedValue) -> PyResult<()>,
{
    let count = types::read_int_length(v.as_slice_mut())
        .map_err(|err| {
            mk_deser_err::<T>(
                typ,
                SetOrListDeserializationErrorKind::LengthDeserializationFailed(
                    DeserializationError::new(err),
                ),
            )
        })
        .map_err(decode_err)?;

    let raw_iter = FixedLengthBytesSequenceIterator::new(count, v);

    for raw in raw_iter {
        let raw = raw.map_err(DeserializationError::new).map_err(decode_err)?;

        let item = T::deserialize_py(elem_typ, raw, py)?;
        builder(item).map_err(py_conv_err)?;
    }

    Ok(())
}

impl<'frame, 'metadata, 'py, T> PyDeserializeValue<'frame, 'metadata, 'py> for List<T>
where
    T: PyDeserializeValue<'frame, 'metadata, 'py>,
{
    fn deserialize_py(
        typ: &'metadata ColumnType<'metadata>,
        v: Option<FrameSlice<'frame>>,
        py: Python<'py>,
    ) -> Result<PyDeserializedValue, DriverDeserializationError> {
        let elem_typ = match typ {
            ColumnType::Collection {
                frozen: _,
                typ: CollectionType::List(elem_typ),
            } => elem_typ,
            _ => {
                return Err(DriverDeserializationError::InternalError(
                    "List deserializer called for non-list column type".to_string(),
                ));
            }
        };

        let Some(v) = v else {
            return Ok(PyDeserializedValue::new(PyList::empty(py).into_any()));
        };

        let list = PyList::empty(py);

        deserialize_sequence::<T, _>(typ, v, py, elem_typ, |item| list.append(item))?;

        Ok(PyDeserializedValue::new(list.into_any()))
    }
}

struct MapIterator<'frame, 'metadata, 'py, K, V> {
    col_typ: &'metadata ColumnType<'metadata>,
    k_typ: &'metadata ColumnType<'metadata>,
    v_typ: &'metadata ColumnType<'metadata>,
    raw_iter: FixedLengthBytesSequenceIterator<'frame>,
    phantom_data_k: PhantomData<K>,
    phantom_data_v: PhantomData<V>,
    py: Python<'py>,
}

impl<'frame, 'metadata, 'py, K, V> MapIterator<'frame, 'metadata, 'py, K, V> {
    fn new(
        col_typ: &'metadata ColumnType<'metadata>,
        k_typ: &'metadata ColumnType<'metadata>,
        v_typ: &'metadata ColumnType<'metadata>,
        count: usize,
        slice: FrameSlice<'frame>,
        py: Python<'py>,
    ) -> Self {
        Self {
            col_typ,
            k_typ,
            v_typ,
            raw_iter: FixedLengthBytesSequenceIterator::new(count, slice),
            phantom_data_k: PhantomData,
            phantom_data_v: PhantomData,
            py,
        }
    }
}
impl<'frame, 'metadata, 'py, K, V> Iterator for MapIterator<'frame, 'metadata, 'py, K, V>
where
    K: PyDeserializeValue<'frame, 'metadata, 'py>,
    V: PyDeserializeValue<'frame, 'metadata, 'py>,
{
    type Item = Result<(PyDeserializedValue, PyDeserializedValue), DriverDeserializationError>;

    fn next(&mut self) -> Option<Self::Item> {
        let raw_k = match self.raw_iter.next()? {
            Ok(raw_k) => raw_k,
            Err(err) => {
                let scylla_err = mk_deser_err::<Self>(
                    self.col_typ,
                    BuiltinDeserializationErrorKind::RawCqlBytesReadError(err),
                );
                return Some(Err(decode_err(scylla_err)));
            }
        };
        let raw_v = match self.raw_iter.next()? {
            Ok(raw_v) => raw_v,
            Err(err) => {
                let scylla_err = mk_deser_err::<Self>(
                    self.col_typ,
                    BuiltinDeserializationErrorKind::RawCqlBytesReadError(err),
                );
                return Some(Err(decode_err(scylla_err)));
            }
        };

        let do_next = || -> Self::Item {
            let k = K::deserialize_py(self.k_typ, raw_k, self.py)?;
            let v = V::deserialize_py(self.v_typ, raw_v, self.py)?;
            Ok((k, v))
        };
        Some(do_next())
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.raw_iter.size_hint()
    }
}

struct Map<K, V> {
    phantom_data_k: PhantomData<K>,
    phantom_data_v: PhantomData<V>,
}

impl<'frame, 'metadata, 'py, K, V> PyDeserializeValue<'frame, 'metadata, 'py> for Map<K, V>
where
    K: PyDeserializeValue<'frame, 'metadata, 'py>,
    V: PyDeserializeValue<'frame, 'metadata, 'py>,
{
    fn deserialize_py(
        typ: &'metadata ColumnType<'metadata>,
        v: Option<FrameSlice<'frame>>,
        py: Python<'py>,
    ) -> Result<PyDeserializedValue, DriverDeserializationError> {
        let (key_typ, value_typ) = match typ {
            ColumnType::Collection {
                frozen: _,
                typ: CollectionType::Map(key_typ, value_typ),
            } => (key_typ, value_typ),
            _ => {
                return Err(DriverDeserializationError::InternalError(
                    "Map deserializer called for non-map column type".to_string(),
                ));
            }
        };

        let Some(mut v) = v else {
            return Ok(PyDeserializedValue::new(PyDict::new(py).into_any()));
        };

        let count = types::read_int_length(v.as_slice_mut())
            .map_err(|err| {
                mk_deser_err::<Self>(
                    typ,
                    MapDeserializationErrorKind::LengthDeserializationFailed(
                        DeserializationError::new(err),
                    ),
                )
            })
            .map_err(decode_err)?;

        let map_iter =
            MapIterator::<'_, '_, '_, K, V>::new(typ, key_typ, value_typ, 2 * count, v, py);
        let dict = PyDict::new(py);

        for item in map_iter {
            let (key, value) = item?;
            dict.set_item(key, value).map_err(py_conv_err)?;
        }

        Ok(PyDeserializedValue::new(dict.into_any()))
    }
}

struct Set<T> {
    phantom_data: PhantomData<T>,
}

impl<'frame, 'metadata, 'py, T> PyDeserializeValue<'frame, 'metadata, 'py> for Set<T>
where
    T: PyDeserializeValue<'frame, 'metadata, 'py>,
{
    fn deserialize_py(
        typ: &'metadata ColumnType<'metadata>,
        v: Option<FrameSlice<'frame>>,
        py: Python<'py>,
    ) -> Result<PyDeserializedValue, DriverDeserializationError> {
        let elem_typ = match typ {
            ColumnType::Collection {
                frozen: _,
                typ: CollectionType::Set(elem_typ),
            } => elem_typ,
            _ => {
                return Err(DriverDeserializationError::InternalError(
                    "Set deserializer called for non-set column type".to_string(),
                ));
            }
        };

        let Some(v) = v else {
            return Ok(PyDeserializedValue::new(
                PySet::empty(py).map_err(py_conv_err)?.into_any(),
            ));
        };

        let set = PySet::empty(py).map_err(py_conv_err)?;

        deserialize_sequence::<T, _>(typ, v, py, elem_typ, |item| set.add(item))?;

        Ok(PyDeserializedValue::new(set.into_any()))
    }
}

struct VectorIterator<'frame, 'metadata, 'py, T> {
    collection_type: &'metadata ColumnType<'metadata>,
    element_type: &'metadata ColumnType<'metadata>,
    remaining: usize,
    element_length: Option<usize>,
    slice: FrameSlice<'frame>,
    phantom_data: PhantomData<T>,
    py: Python<'py>,
}

impl<'frame, 'metadata, 'py, T> VectorIterator<'frame, 'metadata, 'py, T> {
    fn new(
        collection_type: &'metadata ColumnType<'metadata>,
        element_type: &'metadata ColumnType<'metadata>,
        count: usize,
        element_length: Option<usize>,
        slice: FrameSlice<'frame>,
        py: Python<'py>,
    ) -> Self {
        Self {
            collection_type,
            element_type,
            remaining: count,
            element_length,
            slice,
            phantom_data: PhantomData,
            py,
        }
    }
}

impl<'frame, 'metadata, 'py, T> VectorIterator<'frame, 'metadata, 'py, T>
where
    T: PyDeserializeValue<'frame, 'metadata, 'py>,
{
    fn next_constant_length_elem(
        &mut self,
        element_length: usize,
    ) -> Option<<Self as Iterator>::Item> {
        self.remaining = self.remaining.checked_sub(1)?;

        let raw = self
            .slice
            .read_n_bytes(element_length)
            .map_err(|err| {
                mk_deser_err::<Self>(
                    self.collection_type,
                    BuiltinDeserializationErrorKind::RawCqlBytesReadError(err),
                )
            })
            .map_err(decode_err);

        Some(raw.and_then(|raw| T::deserialize_py(self.element_type, raw, self.py)))
    }

    fn next_variable_length_elem(&mut self) -> Option<<Self as Iterator>::Item> {
        self.remaining = self.remaining.checked_sub(1)?;

        let size = types::unsigned_vint_decode(self.slice.as_slice_mut())
            .map_err(|err| {
                mk_deser_err::<Self>(
                    self.collection_type,
                    BuiltinDeserializationErrorKind::RawCqlBytesReadError(
                        LowLevelDeserializationError::IoError(Arc::new(err)),
                    ),
                )
            })
            .map_err(decode_err);

        let raw = size
            .and_then(|size| {
                size.try_into()
                    .map_err(|_| {
                        mk_deser_err::<Self>(
                            self.collection_type,
                            BuiltinDeserializationErrorKind::ValueOverflow,
                        )
                    })
                    .map_err(decode_err)
            })
            .and_then(|size: usize| {
                self.slice
                    .read_n_bytes(size)
                    .map_err(|err| {
                        mk_deser_err::<Self>(
                            self.collection_type,
                            BuiltinDeserializationErrorKind::RawCqlBytesReadError(err),
                        )
                    })
                    .map_err(decode_err)
            });

        Some(raw.and_then(|raw| T::deserialize_py(self.element_type, raw, self.py)))
    }
}

impl<'frame, 'metadata, 'py, T> Iterator for VectorIterator<'frame, 'metadata, 'py, T>
where
    T: PyDeserializeValue<'frame, 'metadata, 'py>,
{
    type Item = Result<PyDeserializedValue, DriverDeserializationError>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.element_length {
            Some(element_length) => self.next_constant_length_elem(element_length),
            None => self.next_variable_length_elem(),
        }
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.remaining, Some(self.remaining))
    }
}

struct Vector<T> {
    phantom_data: PhantomData<T>,
}

impl<'frame, 'metadata, 'py, T> PyDeserializeValue<'frame, 'metadata, 'py> for Vector<T>
where
    T: PyDeserializeValue<'frame, 'metadata, 'py>,
{
    fn deserialize_py(
        typ: &'metadata ColumnType<'metadata>,
        v: Option<FrameSlice<'frame>>,
        py: Python<'py>,
    ) -> Result<PyDeserializedValue, DriverDeserializationError> {
        let (element_type, dimensions) = match typ {
            ColumnType::Vector {
                typ: element_type,
                dimensions,
            } => (element_type, dimensions),
            _ => {
                return Err(DriverDeserializationError::InternalError(
                    "Vector deserializer called for non-vector column type".to_string(),
                ));
            }
        };

        let Some(val) = v else {
            return Ok(PyDeserializedValue::none(py));
        };

        let vector_iterator = VectorIterator::<PyDeserializedValue>::new(
            typ,
            element_type,
            *dimensions as usize,
            element_type.type_size(),
            val,
            py,
        );

        let list = PyList::empty(py);
        for value in vector_iterator {
            list.append(value?).map_err(py_conv_err)?;
        }

        Ok(PyDeserializedValue::new(list.into_any()))
    }
}

fn deser_cql_py_value<'py, 'metadata, 'frame>(
    py: Python<'py>,
    typ: &'metadata ColumnType<'metadata>,
    val: Option<FrameSlice<'frame>>,
) -> Result<PyDeserializedValue, DriverDeserializationError> {
    if let Some(v) = val
        && v.as_slice().is_empty()
    {
        match typ {
            Native(NativeType::Ascii) | Native(NativeType::Blob) | Native(NativeType::Text) => {
                // can't be empty
            }
            _ => return PyDeserializedValue::empty_value(py).map_err(py_conv_err),
        }
    }

    Ok(match typ {
        Native(native_type) => {
            let Some(v) = val else {
                return Ok(PyDeserializedValue::none(py));
            };

            PyDeserializedValue::new({
                match native_type {
                    // CQL Counter → Python int
                    NativeType::Counter => {
                        let v = Counter::deserialize(typ, Some(v)).map_err(decode_err)?;
                        PyInt::new(py, v.0).into_any()
                    }
                    // CQL Decimal → Python decimal.Decimal
                    NativeType::Decimal => {
                        let d: BigDecimal = CqlDecimalBorrowed::deserialize(typ, Some(v))
                            .map_err(decode_err)?
                            .into();
                        d.into_pyobject(py).map_err(py_conv_err)?.into_any()
                    }
                    // CQL TinyInt → Python int
                    NativeType::TinyInt => {
                        let v = i8::deserialize(typ, Some(v)).map_err(decode_err)?;
                        PyInt::new(py, v).into_any()
                    }
                    // CQL SmallInt → Python int
                    NativeType::SmallInt => {
                        let v = i16::deserialize(typ, Some(v)).map_err(decode_err)?;
                        PyInt::new(py, v).into_any()
                    }
                    // CQL Int → Python int
                    NativeType::Int => {
                        let v = i32::deserialize(typ, Some(v)).map_err(decode_err)?;
                        PyInt::new(py, v).into_any()
                    }
                    // CQL BigInt → Python int
                    NativeType::BigInt => {
                        let v = i64::deserialize(typ, Some(v)).map_err(decode_err)?;
                        PyInt::new(py, v).into_any()
                    }
                    // CQL Varint → Python int
                    NativeType::Varint => {
                        let varint: CqlVarintWrapper = CqlVarintBorrowed::deserialize(typ, Some(v))
                            .map_err(decode_err)?
                            .into();
                        varint.into_pyobject(py).map_err(py_conv_err)?.into_any()
                    }
                    // CQL Double → Python float
                    NativeType::Double => {
                        let v = f64::deserialize(typ, Some(v)).map_err(decode_err)?;
                        PyFloat::new(py, v).into_any()
                    }
                    // CQL Float → Python float
                    NativeType::Float => {
                        let v = f32::deserialize(typ, Some(v)).map_err(decode_err)?;
                        PyFloat::new(py, v as f64).into_any()
                    }
                    // CQL Ascii → Python str
                    // CQL Text → Python str
                    NativeType::Ascii | NativeType::Text => {
                        let v = <&str as DeserializeValue<'frame, 'metadata>>::deserialize(
                            typ,
                            Some(v),
                        )
                        .map_err(decode_err)?;
                        PyString::new(py, v).into_any()
                    }
                    // CQL Boolean → Python bool
                    NativeType::Boolean => {
                        let v = bool::deserialize(typ, Some(v)).map_err(decode_err)?;
                        PyBool::new(py, v).to_owned().into_any()
                    }
                    // CQL Date → Python datetime.date
                    NativeType::Date => {
                        let date: chrono_04::NaiveDate = CqlDate::deserialize(typ, Some(v))
                            .map_err(decode_err)?
                            .try_into()
                            .map_err(DeserializationError::new)
                            .map_err(decode_err)?;

                        date.into_pyobject(py).map_err(py_conv_err)?.into_any()
                    }
                    // CQL Timestamp → Python datetime.datetime (UTC)
                    NativeType::Timestamp => {
                        let t: DateTime<Utc> = CqlTimestamp::deserialize(typ, Some(v))
                            .map_err(decode_err)?
                            .try_into()
                            .map_err(DeserializationError::new)
                            .map_err(decode_err)?;

                        t.into_pyobject(py).map_err(py_conv_err)?.into_any()
                    }
                    // CQL Time → Python datetime.time
                    NativeType::Time => {
                        let time: NaiveTime = CqlTime::deserialize(typ, Some(v))
                            .map_err(decode_err)?
                            .try_into()
                            .map_err(DeserializationError::new)
                            .map_err(decode_err)?;
                        time.into_pyobject(py).map_err(py_conv_err)?.into_any()
                    }
                    // CQL Duration → Python dateutil.relativedelta.relativedelta
                    NativeType::Duration => {
                        let d: CqlDurationWrapper = CqlDuration::deserialize(typ, Some(v))
                            .map_err(decode_err)?
                            .into();
                        d.into_pyobject(py).map_err(py_conv_err)?.into_any()
                    }
                    // CQL Blob → Python bytes
                    NativeType::Blob => PyBytes::new(py, v.as_slice()).into_any(),
                    // CQL Inet → Python ipaddress.IPv4Address / ipaddress.IPv6Address
                    NativeType::Inet => {
                        let v = IpAddr::deserialize(typ, Some(v)).map_err(decode_err)?;
                        v.into_pyobject(py).map_err(py_conv_err)?.into_any()
                    }
                    // CQL Uuid → Python uuid.UUID
                    NativeType::Uuid => {
                        let v = uuid::Uuid::deserialize(typ, Some(v)).map_err(decode_err)?;
                        v.into_pyobject(py).map_err(py_conv_err)?.into_any()
                    }
                    // CQL Uuid → Python uuid.UUID
                    NativeType::Timeuuid => {
                        let v: uuid::Uuid = CqlTimeuuid::deserialize(typ, Some(v))
                            .map_err(decode_err)?
                            .into();
                        v.into_pyobject(py).map_err(py_conv_err)?.into_any()
                    }
                    _ => {
                        return Err(DriverDeserializationError::UnsupportedType(format!(
                            "unsupported CQL Native type {native_type:?}"
                        )));
                    }
                }
            })
        }
        ColumnType::Collection {
            frozen: _frozen,
            typ: col_typ,
        } => match col_typ {
            // CQL List → Python list
            CollectionType::List(_type_name) => {
                List::<PyDeserializedValue>::deserialize_py(typ, val, py)?
            }
            // CQL Map → Python dict
            CollectionType::Map(_key_type, _value_type) => {
                Map::<PyDeserializedValue, PyDeserializedValue>::deserialize_py(typ, val, py)?
            }
            // CQL Set → Python set
            CollectionType::Set(_type_name) => {
                Set::<PyDeserializedValue>::deserialize_py(typ, val, py)?
            }
            _ => {
                return Err(DriverDeserializationError::UnsupportedType(format!(
                    "unsupported CQL Native type {col_typ:?}"
                )));
            }
        },
        // CQL UserDefinedType (UDT) → Python dict[str, value]
        ColumnType::UserDefinedType {
            definition: _udt, ..
        } => {
            let Some(v) = val else {
                return Ok(PyDeserializedValue::none(py));
            };

            let iter = UdtIterator::deserialize(typ, Some(v)).map_err(decode_err)?;

            let dict = PyDict::new(py);

            for ((col_name, col_type), res) in iter {
                let v = res.map_err(decode_err)?;

                let val = PyDeserializedValue::deserialize_py(col_type, v.flatten(), py)?;

                dict.set_item(col_name.to_string(), val)
                    .map_err(py_conv_err)?;
            }

            PyDeserializedValue::new(dict.into_any())
        }
        // CQL Vector → Python list
        ColumnType::Vector { .. } => Vector::<PyDeserializedValue>::deserialize_py(typ, val, py)?,
        // CQL Tuple → Python tuple
        ColumnType::Tuple(type_names) => {
            let Some(mut v) = val else {
                return Ok(PyDeserializedValue::none(py));
            };

            let t = type_names.iter().map(|typ| -> PyValueOrError {
                let result: Result<PyDeserializedValue, DriverDeserializationError> = v
                    .read_cql_bytes()
                    // low-level → DeserializationError
                    .map_err(DeserializationError::new)
                    // DeserializationError → DriverDeserializationError
                    .map_err(decode_err)
                    // Option<&[u8]> → PyDeserializedValue
                    .and_then(|raw| PyDeserializedValue::deserialize_py(typ, raw, py));
                PyValueOrError::new(result)
            });

            let tuple = PyTuple::new(py, t).map_err(py_conv_err)?;
            PyDeserializedValue::new(tuple.into_any())
        }
        _ => {
            return Err(DriverDeserializationError::UnsupportedType(format!(
                "unsupported CQL Native type {typ:?}"
            )));
        }
    })
}

#[pymodule]
pub(crate) fn value(_py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<CqlEmpty>()?;

    Ok(())
}
