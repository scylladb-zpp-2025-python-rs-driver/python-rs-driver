use pyo3::prelude::{PyDictMethods, PyListMethods};
use pyo3::types::{PyBool, PyDict, PyInt, PyList, PyString};
use pyo3::{Bound, IntoPyObject, PyAny, Python};
use scylla_cql::_macro_internal::{
    ColumnType, DeserializationError, DeserializeValue, FrameSlice, UdtIterator,
};
use scylla_cql::deserialize::value::FixedLengthBytesSequenceIterator;
use scylla_cql::frame::response::result::{CollectionType, NativeType};
use scylla_cql::frame::types;
use std::convert::Infallible;

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
pub trait PyDeserializeValue<'frame, 'metadata, 'py>: Sized + IntoPyObject<'py> {
    fn deserialize_py(
        typ: &'metadata ColumnType<'metadata>,
        v: Option<FrameSlice<'frame>>,
        py: Python<'py>,
    ) -> Result<PyDeserializedValue<'py>, DeserializationError>;
}

impl<'frame, 'metadata, 'py, T> PyDeserializeValue<'frame, 'metadata, 'py> for Vec<T>
where
    T: PyDeserializeValue<'frame, 'metadata, 'py>,
{
    fn deserialize_py(
        typ: &'metadata ColumnType<'metadata>,
        v: Option<FrameSlice<'frame>>,
        py: Python<'py>,
    ) -> Result<PyDeserializedValue<'py>, DeserializationError> {
        let elem_typ = match typ {
            ColumnType::Collection {
                frozen: false,
                typ: CollectionType::List(elem_typ),
            }
            | ColumnType::Collection {
                frozen: false,
                typ: CollectionType::Set(elem_typ),
            } => elem_typ,
            _ => {
                unreachable!("Should not happen")
            }
        };

        let mut v = if let Some(v) = v {
            v
        } else {
            return Ok(PyDeserializedValue::new(PyList::empty(py).into_any()));
        };

        let count = types::read_int_length(v.as_slice_mut()).map_err(DeserializationError::new)?;

        let raw_iter = FixedLengthBytesSequenceIterator::new(count, v);

        let list = PyList::empty(py);
        for raw_opt in raw_iter {
            let raw = raw_opt.map_err(DeserializationError::new)?;
            let item = T::deserialize_py(elem_typ, raw, py)?;
            list.append(item).map_err(DeserializationError::new)?;
        }

        Ok(PyDeserializedValue::new(list.into_any()))
    }
}

impl<'frame, 'metadata, 'py, T> PyDeserializeValue<'frame, 'metadata, 'py> for Option<T>
where
    T: PyDeserializeValue<'frame, 'metadata, 'py>,
{
    fn deserialize_py(
        typ: &'metadata ColumnType<'metadata>,
        v: Option<FrameSlice<'frame>>,
        py: Python<'py>,
    ) -> Result<PyDeserializedValue<'py>, DeserializationError> {
        match v {
            Some(v) => T::deserialize_py(typ, Some(v), py),
            None => Ok(PyDeserializedValue::new(py.None().bind(py).to_owned())),
        }
    }
}

pub struct PyDeserializedValue<'py> {
    pub value: Bound<'py, PyAny>,
}

impl<'py> PyDeserializedValue<'py> {
    fn new(value: Bound<'py, PyAny>) -> Self {
        Self { value }
    }
}
impl<'py> IntoPyObject<'py> for PyDeserializedValue<'py> {
    type Target = PyAny;
    type Output = Bound<'py, PyAny>;
    type Error = Infallible;
    fn into_pyobject(self, _: Python<'py>) -> Result<Self::Output, Self::Error> {
        Ok(self.value)
    }
}

impl<'frame, 'metadata, 'py> PyDeserializeValue<'frame, 'metadata, 'py>
    for PyDeserializedValue<'py>
{
    fn deserialize_py(
        typ: &'metadata ColumnType<'metadata>,
        v: Option<FrameSlice<'frame>>,
        py: Python<'py>,
    ) -> Result<Self, DeserializationError> {
        let cql = deser_cql_py_value(py, typ, v)?;

        Ok(PyDeserializedValue { value: cql })
    }
}

fn deser_cql_py_value<'py, 'metadata, 'frame>(
    py: Python<'py>,
    typ: &'metadata ColumnType<'metadata>,
    val: Option<FrameSlice<'frame>>,
) -> Result<Bound<'py, PyAny>, DeserializationError> {
    match typ {
        ColumnType::Native(native_type) => match native_type {
            NativeType::Int => {
                let v = i32::deserialize(typ, val)?;
                Ok(PyInt::new(py, v).into_any())
            }
            NativeType::BigInt => {
                let v = i64::deserialize(typ, val)?;
                Ok(PyInt::new(py, v).into_any())
            }
            NativeType::Text => {
                let v = <&str as DeserializeValue<'frame, 'metadata>>::deserialize(typ, val)?;
                Ok(PyString::new(py, v).into_any())
            }
            NativeType::Boolean => {
                let v = bool::deserialize(typ, val)?;
                Ok(PyBool::new(py, v).to_owned().into_any())
            }
            _ => unimplemented!(),
        },
        ColumnType::Collection {
            frozen: _frozen,
            typ: col_typ,
        } => match col_typ {
            CollectionType::List(_type_name) => {
                let list = Vec::<PyDeserializedValue>::deserialize_py(typ, val, py)?;
                list.into_pyobject(py).map_err(DeserializationError::new)
            }
            _ => unimplemented!(),
        },
        ColumnType::UserDefinedType {
            definition: _udt, ..
        } => {
            let iter = UdtIterator::deserialize(typ, val)?;

            let dict = PyDict::new(py);

            for ((col_name, col_type), res) in iter {
                let val = res.and_then(|v| {
                    Option::<PyDeserializedValue>::deserialize_py(col_type, v.flatten(), py)
                })?;

                dict.set_item(col_name.to_string(), val)
                    .map_err(DeserializationError::new)?;
            }

            Ok(dict.into_any())
        }
        _ => unimplemented!(),
    }
}
