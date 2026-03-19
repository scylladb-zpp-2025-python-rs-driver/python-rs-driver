// src/errors.rs

use pyo3::create_exception;
use pyo3::exceptions::PyException;
use pyo3::prelude::*;
use pyo3::types::PyModule;

/* Python exception classes */

create_exception!(errors, ScyllaErrorPy, PyException);

create_exception!(errors, ConnectionErrorPy, ScyllaErrorPy);

create_exception!(errors, SessionConfigErrorPy, ScyllaErrorPy);

// Policy: DriverError types are pure Rust and contain PyErr only as source
// in cases where the error originated from Python code (e.g. during extraction or user callbacks).
// Conversion to PyErr happens at the boundary (e.g. in #[pymethods] implementations)
// using the From<DriverError> for PyErr implementation, which maps each DriverError variant to
// an appropriate Python exception class and attaches any relevant information as attributes or causes.

// For errors originating from Python code, we attach the original PyErr as the cause
// in the PyErr going back to Python, so that users can inspect the original exception type and message if needed.

// For errors originating from the Rust driver, we include the original error message in our custom Python exception
// and attach any relevant structured information as attributes.

// For errors originating from our own Rust code, we create a custom Python exception with a descriptive message,
// and we can include any relevant information in the message or as attributes.

/* Connection errors */

/// Errors that can occur during session creation and connection establishment.
#[derive(Debug)]
#[must_use]
pub enum ConnectionError {
    /// The Tokio task running session creation failed to join.
    RuntimeTaskJoinFailed { message: String },
    /// The Rust driver failed to establish a new session.
    NewSessionError {
        source: Box<scylla::errors::NewSessionError>,
    },
}

impl ConnectionError {
    /* Constructors */

    pub fn runtime_task_join_failed(message: String) -> Self {
        Self::RuntimeTaskJoinFailed { message }
    }

    pub fn new_session_error(source: scylla::errors::NewSessionError) -> Self {
        Self::NewSessionError {
            source: Box::new(source),
        }
    }
}

impl From<ConnectionError> for PyErr {
    fn from(e: ConnectionError) -> PyErr {
        match e {
            ConnectionError::RuntimeTaskJoinFailed { message } => ConnectionErrorPy::new_err(
                format!("Internal driver error: runtime error while creating session: {message}"),
            ),

            ConnectionError::NewSessionError { source } => {
                ConnectionErrorPy::new_err(format!("failed to establish session: {source}"))
            }
        }
    }
}

// Allow converting a tokio::task::JoinError into ConnectionError
// so that callers that spawn tasks can map JoinError -> ConnectionError via the `From` trait.
impl From<tokio::task::JoinError> for ConnectionError {
    fn from(err: tokio::task::JoinError) -> Self {
        ConnectionError::runtime_task_join_failed(err.to_string())
    }
}

/* Session configuration errors */

/// Errors related to invalid session configuration.
#[derive(Debug)]
#[must_use]
pub enum SessionConfigError {
    /// The provided port value is invalid (e.g. not an integer, or out of the valid range).
    InvalidPort { source: Box<PyErr> },
    /// The contact_points argument is of the wrong type (e.g. a string instead of a list).
    ContactPointsTypeError,
    /// try_iter() failed on the contact_points argument.
    ContactPointsNotIterable { source: Box<PyErr> },
    /// Failed to access an item in the contact_points iterable at the given index.
    ContactPointAccessFailed { index: usize, source: Box<PyErr> },
    /// An item in the contact_points iterable is of the wrong type (e.g. not a string).
    ContactPointTypeError { index: usize, source: Box<PyErr> },
    /// Failed to convert an item in the contact_points iterable to a string (e.g. invalid UTF-8).
    ContactPointConversionFailed { index: usize, source: Box<PyErr> },
}

impl SessionConfigError {
    /* Constructors */

    pub fn invalid_port(source: PyErr) -> Self {
        Self::InvalidPort {
            source: Box::new(source),
        }
    }

    pub fn contact_points_type_error() -> Self {
        Self::ContactPointsTypeError
    }

    pub fn contact_points_not_iterable(source: PyErr) -> Self {
        Self::ContactPointsNotIterable {
            source: Box::new(source),
        }
    }

    pub fn contact_point_access_failed(index: usize, source: PyErr) -> Self {
        Self::ContactPointAccessFailed {
            index,
            source: Box::new(source),
        }
    }

    pub fn contact_point_type_error(index: usize, source: PyErr) -> Self {
        Self::ContactPointTypeError {
            index,
            source: Box::new(source),
        }
    }

    pub fn contact_point_conversion_failed(index: usize, source: PyErr) -> Self {
        Self::ContactPointConversionFailed {
            index,
            source: Box::new(source),
        }
    }
}

/// Helper function to build a SessionConfigErrorPy with optional cause and index attributes.
fn build_session_config_pyerr(
    py: Python<'_>,
    message: impl Into<String>,
    cause: Option<PyErr>,
    index: Option<usize>,
) -> PyErr {
    let err = SessionConfigErrorPy::new_err(message.into());

    if let Some(cause) = cause {
        err.set_cause(py, Some(cause));
    }

    let inst = err.value(py);
    if let Some(index) = index {
        let _ = inst.setattr("index", index);
    }

    err
}

impl From<SessionConfigError> for PyErr {
    fn from(e: SessionConfigError) -> PyErr {
        Python::attach(|py| match e {
            SessionConfigError::InvalidPort { source } => {
                let message = "Invalid port value: expected an integer between 0 and 65535.";

                build_session_config_pyerr(py, message, Some(*source), None)
            }

            SessionConfigError::ContactPointsTypeError => {
                let message = "contact_points should be a sequence of strings, not a string!";

                build_session_config_pyerr(py, message, None, None)
            }

            SessionConfigError::ContactPointsNotIterable { source } => {
                let message = "contact_points is not iterable: expected a sequence of strings (e.g. list or tuple) for contact_points";

                build_session_config_pyerr(py, message, Some(*source), None)
            }

            SessionConfigError::ContactPointAccessFailed { index, source } => {
                let message = format!("Failed to access contact point at index {index}");

                build_session_config_pyerr(py, message, Some(*source), Some(index))
            }

            SessionConfigError::ContactPointTypeError { index, source } => {
                let message =
                    format!("Invalid contact point type at index {index}: expected a string");

                build_session_config_pyerr(py, message, Some(*source), Some(index))
            }

            SessionConfigError::ContactPointConversionFailed { index, source } => {
                let message = format!(
                    "Failed to convert contact point at index {index} to string (e.g. invalid UTF-8)"
                );

                build_session_config_pyerr(py, message, Some(*source), Some(index))
            }
        })
    }
}

#[pymodule]
pub(crate) fn errors(py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add("ScyllaError", py.get_type::<ScyllaErrorPy>())?;
    module.add("ConnectionError", py.get_type::<ConnectionErrorPy>())?;
    module.add("SessionConfigError", py.get_type::<SessionConfigErrorPy>())?;
    Ok(())
}
