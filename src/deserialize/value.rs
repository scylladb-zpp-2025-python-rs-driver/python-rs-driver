use crate::deserialize::PyDeserializationError;
use pyo3::types::{PyInt, PyNone};
use pyo3::{Bound, IntoPyObject, Py, PyAny, Python};
use scylla_cql::frame::response::result::{NativeType};
use std::convert::Infallible;
use scylla_cql::_macro_internal::{ColumnType, DeserializeValue};
use scylla_cql::_macro_internal::ColumnType::Native;
use scylla_cql::deserialize::{FrameSlice};

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
    ) -> Result<PyDeserializedValue, PyDeserializationError>;
}

pub(crate) struct PyDeserializedValue {
    value: Py<PyAny>,
}

impl PyDeserializedValue {
    fn new(value: Bound<PyAny>) -> Self {
        Self {
            value: value.into(),
        }
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
    ) -> Result<Self, PyDeserializationError> {
        match v {
            None => Ok(PyDeserializedValue::new(py_none(py))),
            Some(v) => {
                let cql = deser_cql_py_value(py, typ, v)?;
                Ok(PyDeserializedValue::new(cql))
            }
        }
    }
}

fn deser_cql_py_value<'py, 'metadata, 'frame>(
    py: Python<'py>,
    typ: &'metadata ColumnType<'metadata>,
    val: FrameSlice<'frame>,
) -> Result<Bound<'py, PyAny>, PyDeserializationError> {
    if val.as_slice().is_empty() {
        match typ {
            Native(NativeType::Ascii) | Native(NativeType::Blob) | Native(NativeType::Text) => {
                // can't be empty
            }
            _ => return Ok(py_none(py)),
        }
    }

    match typ {
        Native(native_type) => match native_type {
            NativeType::Int => {
                let v = i32::deserialize(typ, Some(val))?;
                Ok(PyInt::new(py, v).into_any())
            }
            _ => unimplemented!(),
        },
        _ => unimplemented!(),
    }
}

fn py_none(py: Python) -> Bound<PyAny> {
    PyNone::get(py).to_owned().into_any()
}
