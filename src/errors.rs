// src/errors.rs
use pyo3::PyErr;
use pyo3::create_exception;
use pyo3::exceptions::PyException;
use pyo3::prelude::*;
use pyo3::types::PyModule;
use pyo3::exceptions::PyRuntimeError;


// Python exception classes
create_exception!(errors, ScyllaError, PyException);

create_exception!(errors, ExecutionErrorPy, ScyllaError);
create_exception!(errors, BadQueryErrorPy, ExecutionErrorPy);
create_exception!(errors, RuntimeErrorPy, PyRuntimeError); // Inherits from PyRuntimeError to pass the tests expecting RuntimeError
create_exception!(errors, ConnectionErrorPy, ExecutionErrorPy);

create_exception!(errors, DeserializationErrorPy, ScyllaError);
create_exception!(errors, UnsupportedTypeErrorPy, DeserializationErrorPy);
create_exception!(errors, DecodeFailedErrorPy, DeserializationErrorPy);
create_exception!(errors, PyConversionFailedErrorPy, DeserializationErrorPy);
create_exception!(errors, InternalErrorPy, DeserializationErrorPy);

// Rust errors
#[derive(Debug)]
pub(crate) enum ExecutionError {
    /// Failed during query execution at runtime.
    Runtime(String),

    /// Failed to establish a session (connect).
    Connect(String),

    /// Query/prepare failed due to invalid query/statement or invalid input.
    BadQuery(String),
}

#[derive(Debug)]
pub(crate) enum DeserializationError {
    /// We hit a CQL type we don't support yet (previously `unimplemented!()`).
    UnsupportedType(String),

    /// From scylla_cql deserialization errors.
    DecodeFailed(String),

    /// PyErr errors during conversion.
    PyConversionFailed(String),

    /// Generic deserialization failure when we don't yet classify it.
    InternalError(String),
}

// Mapping to Python exceptions
impl From<ExecutionError> for PyErr {
    fn from(e: ExecutionError) -> PyErr {
        match e {
            ExecutionError::Runtime(msg) => RuntimeErrorPy::new_err(msg),
            ExecutionError::Connect(msg) => ConnectionErrorPy::new_err(msg),
            ExecutionError::BadQuery(msg) => BadQueryErrorPy::new_err(msg),
        }
    }
}

impl From<DeserializationError> for PyErr {
    fn from(e: DeserializationError) -> PyErr {
        Python::attach(|py| match e {
            DeserializationError::UnsupportedType(msg) => {
                let err = DeserializationErrorPy::new_err("Unsupported CQL type");
                let cause = UnsupportedTypeErrorPy::new_err(msg);
                err.set_cause(py, Some(cause));
                err
            }

            DeserializationError::DecodeFailed(msg) => {
                let err = DeserializationErrorPy::new_err("Failed to deserialize CQL value");
                let cause = DecodeFailedErrorPy::new_err(msg);
                err.set_cause(py, Some(cause));
                err
            }

            DeserializationError::PyConversionFailed(msg) => {
                let err = DeserializationErrorPy::new_err("Failed to convert value to Python");
                let cause = PyConversionFailedErrorPy::new_err(msg);
                err.set_cause(py, Some(cause));
                err
            }

            DeserializationError::InternalError(msg) => {
                let err = DeserializationErrorPy::new_err("Internal deserialization error");
                let cause = InternalErrorPy::new_err(msg);
                err.set_cause(py, Some(cause));
                err
            }
        })
    }
}

/// Helper function to convert scylla_cql decoding errors to our DeserializationError
pub(crate) fn decode_err(e: scylla_cql::deserialize::DeserializationError) -> DeserializationError {
    DeserializationError::DecodeFailed(e.to_string())
}

/// Helper function to convert PyO3 conversion error to our DeserializationError
pub(crate) fn py_conv_err(e: pyo3::PyErr) -> DeserializationError {
    DeserializationError::PyConversionFailed(format_pyerr(&e))
}

pub(crate) fn bad_query_err(e: pyo3::PyErr) -> ExecutionError {
    ExecutionError::BadQuery(format_pyerr(&e))
}

fn format_pyerr(e: &pyo3::PyErr) -> String {
    Python::attach(|py| {
        // name() returns a Python string object (Bound<PyString>) in this PyO3 version
        let ty_name = match e.get_type(py).name() {
            Ok(n) => n.to_string_lossy().into_owned(),
            Err(_) => "UnknownError".to_string(),
        };

        let msg = e.value(py).to_string();

        if msg.is_empty() {
            ty_name
        } else {
            format!("{ty_name}: {msg}")
        }
    })
}

#[pymodule]
pub(crate) fn errors(_py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add("ScyllaError", _py.get_type::<ScyllaError>())?;
    module.add("ExecutionError", _py.get_type::<ExecutionErrorPy>())?;
    module.add("RuntimeError", _py.get_type::<RuntimeErrorPy>())?;
    module.add("BadQueryError", _py.get_type::<BadQueryErrorPy>())?;
    module.add("ConnectionError", _py.get_type::<ConnectionErrorPy>())?;
    module.add(
        "DeserializationError",
        _py.get_type::<DeserializationErrorPy>(),
    )?;
    module.add(
        "UnsupportedTypeError",
        _py.get_type::<UnsupportedTypeErrorPy>(),
    )?;
    module.add("DecodeFailedError", _py.get_type::<DecodeFailedErrorPy>())?;
    module.add(
        "PyConversionFailedError",
        _py.get_type::<PyConversionFailedErrorPy>(),
    )?;
    module.add("InternalError", _py.get_type::<InternalErrorPy>())?;
    Ok(())
}
