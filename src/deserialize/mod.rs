use pyo3::PyErr;
use pyo3::exceptions::PyRuntimeError;
use scylla_cql::deserialize::DeserializationError;

pub mod value;
pub mod results;

// NOTE:
// This is temporary / placeholder error handling used to unblock the current work.
// It will be replaced once we agree on a proper, final error-handling strategy.
#[derive(Debug)]
pub struct PyDeserializationError {
    inner: PyErr,
}

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
