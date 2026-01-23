use pyo3::PyErr;
use pyo3::exceptions::PyRuntimeError;
use scylla_cql::deserialize::DeserializationError;
use std::error::Error;
use std::fmt::{Display, Formatter};

mod conversion;
pub mod results;
pub mod value;

// NOTE:
// This is temporary / placeholder error handling used to unblock the current work.
// It will be replaced once we agree on a proper, final error-handling strategy.
#[derive(Debug)]
pub struct PyDeserializationError {
    inner: PyErr,
}

impl Display for PyDeserializationError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.inner)
    }
}

impl Error for PyDeserializationError {}
impl PyDeserializationError {
    pub fn new(err: PyErr) -> Self {
        Self { inner: err }
    }
}

impl From<PyErr> for PyDeserializationError {
    fn from(err: PyErr) -> Self {
        Self::new(err)
    }
}

impl From<PyDeserializationError> for PyErr {
    fn from(err: PyDeserializationError) -> PyErr {
        err.inner
    }
}

impl From<DeserializationError> for PyDeserializationError {
    fn from(err: DeserializationError) -> Self {
        let py_err = PyRuntimeError::new_err(err.to_string());
        PyDeserializationError::new(py_err)
    }
}

trait IntoPyDeserError {
    fn into_py_deser(self) -> PyDeserializationError;
}

impl<T> IntoPyDeserError for T
where
    T: Error + Send + Sync + 'static,
{
    fn into_py_deser(self) -> PyDeserializationError {
        let py_err = PyRuntimeError::new_err(self.to_string());
        PyDeserializationError::new(py_err)
    }
}
