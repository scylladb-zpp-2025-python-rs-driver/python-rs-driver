// src/errors.rs
use pyo3::create_exception;
use pyo3::exceptions::PyException;
use pyo3::prelude::*;

// Python exception classes
create_exception!(errors, ScyllaError, PyException);

// create_exception!(errors, ExecutionErrorPy, ScyllaError);
// create_exception!(errors, BadQueryErrorPy, ExecutionErrorPy);
// create_exception!(errors, TimeoutErrorPy, ExecutionErrorPy);
// create_exception!(errors, ConnectionErrorPy, ExecutionErrorPy);

create_exception!(errors, DeserializationErrorPy, ScyllaError);
create_exception!(errors, UnsupportedTypeErrorPy, DeserializationErrorPy);
create_exception!(errors, DecodeFailedErrorPy, DeserializationErrorPy);
create_exception!(errors, PyConversionFailedErrorPy, DeserializationErrorPy);
create_exception!(errors, InternalErrorPy, DeserializationErrorPy);

// Rust errors
// #[derive(Debug)]
// pub(crate) enum ExecutionError {
//     /// Generic execution/connect/prepare failure when we don't yet classify it.
//     Other(String),

//     /// Failed to establish a session (connect).
//     Connect(String),

//     /// Query/prepare failed due to invalid query/statement or invalid input.
//     BadQuery(String),

//     /// Explicit request timeout exceeded.
//     Timeout(Duration),
// }

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
// impl From<ExecutionError> for PyErr {
//     fn from(e: ExecutionError) -> PyErr {
//         match e {
//             ExecutionError::Other(msg) => ExecutionErrorPy::new_err(msg),
//             ExecutionError::Connect(msg) => ConnectionErrorPy::new_err(msg),
//             ExecutionError::BadQuery(msg) => BadQueryErrorPy::new_err(msg),
//             ExecutionError::Timeout(dur) => TimeoutErrorPy::new_err(format!(
//                 "Request execution exceeded timeout of {}ms",
//                 dur.as_millis()
//             )),
//         }
//     }
// }

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
    DeserializationError::PyConversionFailed(e.to_string())
}

#[pymodule]
pub(crate) fn errors(_py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add("ScyllaError", _py.get_type::<ScyllaError>())?;
    // module.add("ExecutionError", _py.get_type::<ExecutionErrorPy>())?;
    // module.add("BadQueryError", _py.get_type::<BadQueryErrorPy>())?;
    // module.add("TimeoutError", _py.get_type::<TimeoutErrorPy>())?;
    // module.add("ConnectionError", _py.get_type::<ConnectionErrorPy>())?;
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
