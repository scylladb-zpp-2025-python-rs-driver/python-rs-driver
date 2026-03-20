// src/errors.rs

use pyo3::create_exception;
use pyo3::exceptions::PyException;
use pyo3::prelude::*;
use pyo3::types::PyModule;

/* Python exception classes */

create_exception!(errors, ScyllaErrorPy, PyException);

create_exception!(errors, ConnectionErrorPy, ScyllaErrorPy);

create_exception!(errors, SessionConfigErrorPy, ScyllaErrorPy);

create_exception!(errors, StatementConversionErrorPy, ScyllaErrorPy);

create_exception!(errors, ExecuteErrorPy, ScyllaErrorPy);

create_exception!(errors, PrepareErrorPy, ScyllaErrorPy);

create_exception!(errors, SchemaAgreementErrorPy, ScyllaErrorPy);

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

/// Errors that can occur during conversion of Python objects into statements for execution.
#[derive(Debug)]
#[must_use]
pub enum StatementConversionError {
    /// The provided statement argument is of an unsupported type.
    InvalidStatementType { got: String },
    /// Failed to convert a Python string object into a Rust string when extracting a statement.
    StatementStringConversionFailed { source: Box<PyErr> },
}

impl StatementConversionError {
    /* Constructors */

    pub fn invalid_statement_type(got: String) -> Self {
        Self::InvalidStatementType { got }
    }

    pub fn statement_string_conversion_failed(source: PyErr) -> Self {
        Self::StatementStringConversionFailed {
            source: Box::new(source),
        }
    }
}

impl From<StatementConversionError> for PyErr {
    fn from(e: StatementConversionError) -> PyErr {
        Python::attach(|py| match e {
            StatementConversionError::InvalidStatementType { got } => {
                StatementConversionErrorPy::new_err(format!(
                    "Invalid statement type: expected a str, Statement, or PreparedStatement, got {got}"
                ))
            }

            StatementConversionError::StatementStringConversionFailed { source } => {
                let err = StatementConversionErrorPy::new_err(
                    "Failed to convert statement string to Rust string",
                );

                err.set_cause(py, Some(*source));
                err
            }
        })
    }
}

/// Errors that can occur during execution of a query (session.execute),
/// excluding deserialization errors which are represented separately in RowIterationError.
#[derive(Debug)]
#[must_use]
pub enum ExecuteError {
    /// paging_state parameter in session.execute must be None.
    PagingStateMustBeNoneForUnpagedExecution,
    /// The Rust driver failed while executing a query.
    RustDriverExecutionError {
        source: Box<scylla::errors::ExecutionError>,
    },
    /// The Tokio runtime task responsible for executing the query failed to join.
    RuntimeTaskJoinFailed { message: Box<str> },
}

impl ExecuteError {
    /* Constructors */

    pub fn paging_state_must_be_none_for_unpaged_execution() -> Self {
        Self::PagingStateMustBeNoneForUnpagedExecution
    }

    pub fn rust_driver_execution_error(source: scylla::errors::ExecutionError) -> Self {
        Self::RustDriverExecutionError {
            source: Box::new(source),
        }
    }

    pub fn runtime_task_join_failed(err: tokio::task::JoinError) -> Self {
        Self::RuntimeTaskJoinFailed {
            message: err.to_string().into_boxed_str(),
        }
    }
}

impl From<ExecuteError> for PyErr {
    fn from(e: ExecuteError) -> PyErr {
        match e {
            ExecuteError::PagingStateMustBeNoneForUnpagedExecution => {
                ExecuteErrorPy::new_err("Paging state must be None for unpaged execution")
            }

            ExecuteError::RustDriverExecutionError { source } => {
                let message = format!("Failed to execute statement: {source}");

                ExecuteErrorPy::new_err(message)
            }

            ExecuteError::RuntimeTaskJoinFailed { message } => ExecuteErrorPy::new_err(format!(
                "Internal driver error: runtime error while executing query: {message}"
            )),
        }
    }
}

// Allow converting a tokio::task::JoinError into ExecuteError
// so that callers that spawn tasks can map JoinError -> ExecuteError via the `From` trait.
impl From<tokio::task::JoinError> for ExecuteError {
    fn from(err: tokio::task::JoinError) -> Self {
        // Use the existing constructor which accepts JoinError
        ExecuteError::runtime_task_join_failed(err)
    }
}

/// Errors that can occur during preparation of a statement.
#[derive(Debug)]
#[must_use]
pub enum PrepareError {
    /// The Rust driver failed while preparing a statement.
    #[allow(clippy::enum_variant_names)]
    RustDriverPrepareError {
        source: Box<scylla::errors::PrepareError>,
    },
    /// Attempted to prepare an already prepared statement.
    CannotPreparePreparedStatement,
}

impl PrepareError {
    /* Constructors */

    pub fn rust_driver_prepare_error(source: scylla::errors::PrepareError) -> Self {
        Self::RustDriverPrepareError {
            source: Box::new(source),
        }
    }

    pub fn cannot_prepare_prepared_statement() -> Self {
        Self::CannotPreparePreparedStatement
    }
}

impl From<PrepareError> for PyErr {
    fn from(e: PrepareError) -> PyErr {
        match e {
            PrepareError::RustDriverPrepareError { source } => {
                let message = format!("Failed to prepare statement: {source}");

                PrepareErrorPy::new_err(message)
            }

            PrepareError::CannotPreparePreparedStatement => PrepareErrorPy::new_err(
                "Cannot prepare a PreparedStatement; expected a str or Statement",
            ),
        }
    }
}

/// Errors that can occur during schema agreement checks.
#[derive(Debug)]
#[must_use]
pub enum SchemaAgreementError {
    /// The Rust driver failed to check for schema agreement.
    RustDriverSchemaAgreementError {
        source: Box<scylla::errors::SchemaAgreementError>,
    },
    /// The Tokio runtime task responsible for checking schema agreement failed to join.
    RuntimeTaskJoinFailed { message: Box<str> },
}

impl SchemaAgreementError {
    /* Constructors */

    pub fn rust_driver_schema_agreement_error(
        source: scylla::errors::SchemaAgreementError,
    ) -> Self {
        Self::RustDriverSchemaAgreementError {
            source: Box::new(source),
        }
    }

    pub fn runtime_task_join_failed(err: tokio::task::JoinError) -> Self {
        Self::RuntimeTaskJoinFailed {
            message: err.to_string().into_boxed_str(),
        }
    }
}

impl From<SchemaAgreementError> for PyErr {
    fn from(e: SchemaAgreementError) -> PyErr {
        match e {
            SchemaAgreementError::RustDriverSchemaAgreementError { source } => {
                let message = format!("Failed to check schema agreement: {source}");

                SchemaAgreementErrorPy::new_err(message)
            }

            SchemaAgreementError::RuntimeTaskJoinFailed { message } => {
                SchemaAgreementErrorPy::new_err(format!(
                    "Internal driver error: runtime error while checking schema agreement: {message}"
                ))
            }
        }
    }
}

// Allow converting a tokio::task::JoinError into SchemaAgreementError
// so that callers that spawn tasks can map JoinError -> SchemaAgreementError via the `From` trait.
impl From<tokio::task::JoinError> for SchemaAgreementError {
    fn from(err: tokio::task::JoinError) -> Self {
        SchemaAgreementError::runtime_task_join_failed(err)
    }
}

#[pymodule]
pub(crate) fn errors(py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add("ScyllaError", py.get_type::<ScyllaErrorPy>())?;
    module.add("ConnectionError", py.get_type::<ConnectionErrorPy>())?;
    module.add("SessionConfigError", py.get_type::<SessionConfigErrorPy>())?;
    module.add(
        "StatementConversionError",
        py.get_type::<StatementConversionErrorPy>(),
    )?;
    module.add("PrepareError", py.get_type::<PrepareErrorPy>())?;
    module.add(
        "SchemaAgreementError",
        py.get_type::<SchemaAgreementErrorPy>(),
    )?;
    module.add("ExecuteError", py.get_type::<ExecuteErrorPy>())?;
    Ok(())
}
