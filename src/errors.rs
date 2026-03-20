// src/errors.rs
use std::error::Error;
use std::fmt;

use pyo3::PyErr;
use pyo3::create_exception;
use pyo3::exceptions::PyException;
use pyo3::prelude::*;
use pyo3::types::PyModule;

/* Python exception classes */

create_exception!(errors, ScyllaErrorPy, PyException);

create_exception!(errors, RowIterationErrorPy, ScyllaErrorPy);

create_exception!(errors, DeserializationErrorPy, ScyllaErrorPy);
create_exception!(
    errors,
    UnsupportedTypeDeserializationErrorPy,
    DeserializationErrorPy
);
create_exception!(errors, DecodeFailedErrorPy, DeserializationErrorPy);
create_exception!(errors, PyConversionFailedErrorPy, DeserializationErrorPy);

create_exception!(errors, ConnectionErrorPy, ScyllaErrorPy);

create_exception!(errors, SessionConfigErrorPy, ScyllaErrorPy);

create_exception!(errors, StatementConversionErrorPy, ScyllaErrorPy);

create_exception!(errors, ExecuteErrorPy, ScyllaErrorPy);

create_exception!(errors, PrepareErrorPy, ScyllaErrorPy);

create_exception!(errors, SchemaAgreementErrorPy, ScyllaErrorPy);
create_exception!(errors, StatementConfigErrorPy, ScyllaErrorPy);

create_exception!(errors, BatchErrorPy, ScyllaErrorPy);

create_exception!(errors, SerializationErrorPy, ScyllaErrorPy);
create_exception!(
    errors,
    UnsupportedTypeSerializationErrorPy,
    SerializationErrorPy
);
create_exception!(
    errors,
    TypeMismatchSerializationErrorPy,
    SerializationErrorPy
);
create_exception!(
    errors,
    ValueOverflowSerializationErrorPy,
    SerializationErrorPy
);
create_exception!(errors, SerializeFailedErrorPy, SerializationErrorPy);
create_exception!(errors, PySerializationFailedErrorPy, SerializationErrorPy);

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

/* Row iteration errors */

#[derive(Debug)]
pub enum RowIterationError {
    /// An error occurred during deserialization of a CQL value into a Python object.
    Deserialization(DriverDeserializationError),
    /// An error occurred while fetching the next page of results from the Rust driver during iteration.
    FailedToFetchNextPage(ExecuteError),
    /// An error occurred in Python code during processing of a row.
    PythonError(PyErr),
}

impl From<RowIterationError> for PyErr {
    fn from(e: RowIterationError) -> PyErr {
        match e {
            RowIterationError::Deserialization(e) => e.into(),
            RowIterationError::FailedToFetchNextPage(e) => {
                // Add extra context while preserving the original ExecuteErrorPy as cause
                Python::attach(|py| {
                    let err = RowIterationErrorPy::new_err(
                        "Row iteration error: failed to fetch next page of results",
                    );

                    // Wrap the inner ExecuteErrorPy as the cause
                    let cause: PyErr = e.into();
                    err.set_cause(py, Some(cause));
                    err
                })
            }
            RowIterationError::PythonError(e) => {
                Python::attach(|py| {
                    let err = RowIterationErrorPy::new_err(
                        "Row iteration error: a Python error occurred during processing of a row",
                    );

                    // Attach original python exception as cause
                    err.set_cause(py, Some(e));

                    err
                })
            }
        }
    }
}

/* Deserialization errors */

/// Errors that can occur during deserialization of CQL values into Python objects.
#[derive(Debug)]
#[must_use]
pub struct DriverDeserializationError {
    pub kind: DeserializationErrorKind,
    pub location: DeserializationErrorLocation,
}

/// Structured information about where in the data the deserialization error occurred,
/// to provide better context in error messages and for debugging.
#[derive(Debug, Clone, Default)]
pub struct DeserializationErrorLocation {
    pub column_name: Option<Box<str>>,
    pub column_index: Option<usize>,
    pub inner: Box<[InnerSegment]>,
}

/// Represents a segment in the path to the value that failed to deserialize, for nested structures.
#[derive(Debug, Clone)]
pub enum InnerSegment {
    /// An index into a sequence (list/set) where the error occurred.
    SequenceIndex(usize),
    /// An index into a map where the error occurred.
    MapIndex(usize),
    /// An index into a tuple where the error occurred.
    TupleIndex(usize),
    /// A field name in a UDT where the error occurred.
    UdtField(Box<str>),
    /// An index into a vector where the error occurred.
    VectorIndex(usize),
}

#[derive(Debug)]
pub enum DeserializationErrorKind {
    /// The CQL type is not supported by the deserializer
    /// (e.g. an unknown custom type, or a new type added in Scylla that we haven't implemented yet).
    UnsupportedType { cql: Box<str> },
    /// An error occurred during deserialization in the Rust driver.
    ScyllaDecodeFailed {
        source: scylla::deserialize::DeserializationError,
    },
    /// An error occurred during conversion to a Python object
    /// (e.g. invalid UTF-8, unsupported type for Python conversion, etc.).
    PythonConversionFailed { source: Box<pyo3::PyErr> },
    /// Driver invariant violated: a deserializer was called for a mismatched ColumnType.
    /// This indicates a bug in our dispatch logic.
    WrongDeserializer { message: Box<str> },
}

impl DriverDeserializationError {
    /* Constructors */

    pub fn unsupported_type(cql: impl Into<Box<str>>) -> Self {
        Self {
            kind: DeserializationErrorKind::UnsupportedType { cql: cql.into() },
            location: DeserializationErrorLocation::default(),
        }
    }

    pub fn scylla_decode_failed(source: scylla::deserialize::DeserializationError) -> Self {
        Self {
            kind: DeserializationErrorKind::ScyllaDecodeFailed { source },
            location: DeserializationErrorLocation::default(),
        }
    }

    pub fn python_conversion_failed(source: pyo3::PyErr) -> Self {
        Self {
            kind: DeserializationErrorKind::PythonConversionFailed {
                source: Box::new(source),
            },
            location: DeserializationErrorLocation::default(),
        }
    }

    pub fn wrong_deserializer(expected: &'static str, got: impl Into<Box<str>>) -> Self {
        let got = got.into();
        let message = format!(
            "Internal driver error: wrong deserializer selected (expected {expected}, got {got})"
        );

        Self {
            kind: DeserializationErrorKind::WrongDeserializer {
                message: message.into_boxed_str(),
            },
            location: DeserializationErrorLocation::default(),
        }
    }

    /* Column setters */

    pub fn at_column_name(mut self, name: impl Into<Box<str>>) -> Self {
        self.location.column_name = Some(name.into());
        self
    }

    pub fn at_column_index(mut self, index: usize) -> Self {
        self.location.column_index = Some(index);
        self
    }

    /* Inner path pushers (nesting) */

    fn push_inner(&mut self, segment: InnerSegment) {
        let mut v = self.location.inner.to_vec();
        v.push(segment);
        self.location.inner = v.into_boxed_slice();
    }

    pub fn in_sequence_index(mut self, index: usize) -> Self {
        self.push_inner(InnerSegment::SequenceIndex(index));
        self
    }

    pub fn in_map_index(mut self, index: usize) -> Self {
        self.push_inner(InnerSegment::MapIndex(index));
        self
    }

    pub fn in_tuple_index(mut self, index: usize) -> Self {
        self.push_inner(InnerSegment::TupleIndex(index));
        self
    }

    pub fn in_udt_field(mut self, field: impl Into<Box<str>>) -> Self {
        self.push_inner(InnerSegment::UdtField(field.into()));
        self
    }

    pub fn in_vector_index(mut self, index: usize) -> Self {
        self.push_inner(InnerSegment::VectorIndex(index));
        self
    }
}

/// Helper function to format the location information into a human-readable string for error messages.
fn format_location(loc: &DeserializationErrorLocation) -> String {
    let mut parts: Vec<String> = Vec::new();

    if let Some(col) = &loc.column_name {
        parts.push(format!("column_name={col}"));
    }

    if let Some(index) = &loc.column_index {
        parts.push(format!("column_index={index}"));
    }

    for seg in &loc.inner {
        let s = match seg {
            InnerSegment::SequenceIndex(i) => format!("sequence[{i}]"),
            InnerSegment::MapIndex(i) => format!("map[{i}]"),
            InnerSegment::TupleIndex(i) => format!("tuple[{i}]"),
            InnerSegment::UdtField(f) => format!("udt.{f}"),
            InnerSegment::VectorIndex(i) => format!("vector[{i}]"),
        };
        parts.push(s);
    }

    if parts.is_empty() {
        String::new()
    } else {
        format!(" ({})", parts.join(" -> "))
    }
}

fn attach_deserialization_error_location<'py>(
    err: &Bound<'_, pyo3::exceptions::PyBaseException>,
    location: &DeserializationErrorLocation,
    _py: Python<'py>,
) {
    if let Some(col_name) = &location.column_name {
        let _ = err.setattr("column_name", col_name.to_string());
    }

    if let Some(col_index) = &location.column_index {
        let _ = err.setattr("column_index", *col_index);
    }

    // We can also attach the inner path as a list of strings for easier inspection in Python
    let inner_path: Vec<String> = location
        .inner
        .iter()
        .map(|seg| match seg {
            InnerSegment::SequenceIndex(i) => format!("sequence[{i}]"),
            InnerSegment::MapIndex(i) => format!("map[{i}]"),
            InnerSegment::TupleIndex(i) => format!("tuple[{i}]"),
            InnerSegment::UdtField(f) => format!("udt.{f}"),
            InnerSegment::VectorIndex(i) => format!("vector[{i}]"),
        })
        .collect();
    let _ = err.setattr("inner_path", inner_path);
}

/// Helper function to build a deserialization error PyErr with optional cause and location information attached.
fn build_deserialization_pyerr(
    py: Python<'_>,
    err: PyErr,
    location: &DeserializationErrorLocation,
    cause: Option<PyErr>,
) -> PyErr {
    if let Some(cause) = cause {
        err.set_cause(py, Some(cause));
    }

    attach_deserialization_error_location(err.value(py), location, py);
    err
}

impl From<DriverDeserializationError> for PyErr {
    fn from(e: DriverDeserializationError) -> PyErr {
        Python::attach(|py| {
            let location_as_string = format_location(&e.location);

            match e.kind {
                DeserializationErrorKind::UnsupportedType { cql } => {
                    let message = if location_as_string.is_empty() {
                        format!("Unsupported CQL type: {cql}")
                    } else {
                        format!("Unsupported CQL type: {cql}{location_as_string}")
                    };

                    build_deserialization_pyerr(
                        py,
                        UnsupportedTypeDeserializationErrorPy::new_err(message),
                        &e.location,
                        None,
                    )
                }

                DeserializationErrorKind::ScyllaDecodeFailed { source } => {
                    let base = source.to_string();
                    let message = if location_as_string.is_empty() {
                        base
                    } else {
                        format!("{base}{location_as_string}")
                    };

                    build_deserialization_pyerr(
                        py,
                        DecodeFailedErrorPy::new_err(message),
                        &e.location,
                        None,
                    )
                }

                DeserializationErrorKind::PythonConversionFailed { source } => {
                    let message = if location_as_string.is_empty() {
                        "Python conversion failed".to_string()
                    } else {
                        format!("Python conversion failed{location_as_string}")
                    };

                    build_deserialization_pyerr(
                        py,
                        PyConversionFailedErrorPy::new_err(message),
                        &e.location,
                        Some(*source),
                    )
                }

                DeserializationErrorKind::WrongDeserializer { message } => {
                    let message = if location_as_string.is_empty() {
                        message.to_string()
                    } else {
                        format!("{message}{location_as_string}")
                    };

                    build_deserialization_pyerr(
                        py,
                        PyConversionFailedErrorPy::new_err(message),
                        &e.location,
                        None,
                    )
                }
            }
        })
    }
}

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

/// Errors related to invalid statement configuration.
#[derive(Debug)]
#[must_use]
pub enum StatementConfigError {
    /// The provided request timeout is not a positive finite number of seconds.
    InvalidRequestTimeout { value: f64 },
    /// Failed to convert the provided request timeout value into a valid duration.
    RequestTimeoutConversionFailed { value: f64 },
}

impl StatementConfigError {
    /* Constructors */

    pub fn invalid_request_timeout(value: f64) -> Self {
        Self::InvalidRequestTimeout { value }
    }

    pub fn request_timeout_conversion_failed(value: f64) -> Self {
        Self::RequestTimeoutConversionFailed { value }
    }
}

impl From<StatementConfigError> for PyErr {
    fn from(e: StatementConfigError) -> PyErr {
        match e {
            StatementConfigError::InvalidRequestTimeout { value } => {
                StatementConfigErrorPy::new_err(format!(
                    "timeout must be a positive, finite number (in seconds), got {value}"
                ))
            }
            StatementConfigError::RequestTimeoutConversionFailed { value } => {
                StatementConfigErrorPy::new_err(format!(
                    "Failed to convert timeout value {value} to a valid duration"
                ))
            }
        }
    }
}

/// Errors related to batch execution and batch statement configuration.
#[derive(Debug)]
#[must_use]
pub enum BatchError {
    /// The provided request timeout is not a positive finite number of seconds.
    InvalidRequestTimeout { value: f64 },
    /// Failed to convert the provided request timeout value into a valid duration.
    RequestTimeoutConversionFailed { value: f64 },
    /// An error occurred in Python code while handling a batch value.
    PythonConversionFailed { source: Box<PyErr> },
}

impl BatchError {
    /* Constructors */

    pub fn invalid_request_timeout(value: f64) -> Self {
        Self::InvalidRequestTimeout { value }
    }

    pub fn request_timeout_conversion_failed(value: f64) -> Self {
        Self::RequestTimeoutConversionFailed { value }
    }

    pub fn python_conversion_failed(source: PyErr) -> Self {
        Self::PythonConversionFailed {
            source: Box::new(source),
        }
    }
}

impl From<BatchError> for PyErr {
    fn from(e: BatchError) -> PyErr {
        match e {
            BatchError::InvalidRequestTimeout { value } => BatchErrorPy::new_err(format!(
                "timeout must be a positive, finite number (in seconds), got {value}"
            )),
            BatchError::RequestTimeoutConversionFailed { value } => BatchErrorPy::new_err(format!(
                "Failed to convert timeout value {value} to a valid duration"
            )),
            BatchError::PythonConversionFailed { source } => Python::attach(|py| {
                let err =
                    BatchErrorPy::new_err("Python conversion failed while handling batch value");

                err.set_cause(py, Some(*source));
                err
            }),
        }
    }
}

/* Serialization errors */

/// Errors that can occur during serialization of Python values into CQL values.
#[derive(Debug)]
#[must_use]
pub struct DriverSerializationError {
    pub kind: SerializationErrorKind,
    pub location: Option<ParameterReference>,
}

#[derive(Debug)]
pub enum SerializationErrorKind {
    /// Represents a segment in the path to the value that failed to serialize.
    UnsupportedType { cql: Box<str> },
    /// The Python value has the wrong top-level shape for the target CQL type.
    TypeMismatch { expected: TypeExpected },
    /// The Python value could not fit into the requested CQL representation.
    ValueOverflow,
    /// An error occurred while interacting with Python objects during serialization.
    PythonInteropFailed { source: Box<PyErr> },
    /// An error occurred in the Rust driver's serialization layer.
    ScyllaSerializeFailed {
        source: scylla::serialize::SerializationError,
    },
}

/// References a parameter that failed to serialize, either by index or by name.
#[derive(Debug)]
pub enum ParameterReference {
    Index(usize),
    Name(Box<str>),
}

#[derive(Debug)]
pub enum TypeExpected {
    /// Expected a list of values for a CQL list or set.
    List,
    /// Expected a tuple of values for a CQL tuple.
    Tuple,
    /// Expected an iterable of numbers for a CQL vector.
    Vector,
    /// Expected a set of values for a CQL set.
    Set,
    /// Expected a map for a CQL map.
    Map,
    /// Expected a user-defined type (Udt) for a CQL Udt.
    Udt,
}

impl fmt::Display for TypeExpected {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TypeExpected::List => write!(f, "list"),
            TypeExpected::Tuple => write!(f, "tuple"),
            TypeExpected::Vector => write!(f, "vector"),
            TypeExpected::Set => write!(f, "set"),
            TypeExpected::Map => write!(f, "map"),
            TypeExpected::Udt => write!(f, "Udt"),
        }
    }
}

impl fmt::Display for DriverSerializationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let location = format_serialization_location(&self.location);

        match &self.kind {
            SerializationErrorKind::UnsupportedType { cql } => {
                if location.is_empty() {
                    write!(f, "Unsupported CQL type: {cql}")
                } else {
                    write!(f, "Unsupported CQL type: {cql}{location}")
                }
            }
            SerializationErrorKind::TypeMismatch { expected } => {
                if location.is_empty() {
                    write!(f, "Type mismatch: expected {expected}")
                } else {
                    write!(f, "Type mismatch: expected {expected}{location}")
                }
            }
            SerializationErrorKind::ValueOverflow => {
                if location.is_empty() {
                    write!(f, "Value overflow during serialization")
                } else {
                    write!(f, "Value overflow during serialization{location}")
                }
            }
            SerializationErrorKind::PythonInteropFailed { source } => {
                if location.is_empty() {
                    write!(f, "Python serialization failed: {source}")
                } else {
                    write!(f, "Python serialization failed: {source}{location}")
                }
            }
            SerializationErrorKind::ScyllaSerializeFailed { source } => {
                if location.is_empty() {
                    write!(f, "{source}")
                } else {
                    write!(f, "{source}{location}")
                }
            }
        }
    }
}

impl Error for DriverSerializationError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match &self.kind {
            SerializationErrorKind::PythonInteropFailed { source } => Some(source.as_ref()),
            SerializationErrorKind::ScyllaSerializeFailed { source } => Some(source),
            _ => None,
        }
    }
}

impl DriverSerializationError {
    /* Constructors */

    pub fn unsupported_type(cql: impl Into<Box<str>>) -> Self {
        Self {
            kind: SerializationErrorKind::UnsupportedType { cql: cql.into() },
            location: None,
        }
    }

    pub fn type_mismatch(expected: TypeExpected) -> Self {
        Self {
            kind: SerializationErrorKind::TypeMismatch { expected },
            location: None,
        }
    }

    pub fn value_overflow() -> Self {
        Self {
            kind: SerializationErrorKind::ValueOverflow,
            location: None,
        }
    }

    pub fn scylla_serialize_failed(source: scylla::serialize::SerializationError) -> Self {
        Self {
            kind: SerializationErrorKind::ScyllaSerializeFailed { source },
            location: None,
        }
    }

    pub fn python_interop_failed(source: PyErr) -> Self {
        Self {
            kind: SerializationErrorKind::PythonInteropFailed {
                source: Box::new(source),
            },
            location: None,
        }
    }

    /* Top-level location setters */

    pub fn at_parameter_index(mut self, index: usize) -> Self {
        self.location = Some(ParameterReference::Index(index));
        self
    }

    pub fn at_parameter_name(mut self, name: impl Into<Box<str>>) -> Self {
        self.location = Some(ParameterReference::Name(name.into()));
        self
    }
}

/// Helper function to format serialization location information into a readable string.
fn format_serialization_location(loc: &Option<ParameterReference>) -> String {
    let mut parts: Vec<String> = Vec::new();

    if let Some(parameter) = &loc {
        match parameter {
            ParameterReference::Index(i) => parts.push(format!("parameter_index={i}")),
            ParameterReference::Name(n) => parts.push(format!("parameter={n}")),
        }
    }

    if parts.is_empty() {
        String::new()
    } else {
        format!(" ({})", parts.join(" -> "))
    }
}

/// Attaches serialization location attributes to the given Python exception instance.
fn attach_serialization_location_attrs(
    py: Python<'_>,
    err: &Bound<'_, pyo3::exceptions::PyBaseException>,
    loc: &Option<ParameterReference>,
) {
    match &loc {
        Some(ParameterReference::Index(i)) => {
            let _ = err.setattr("parameter", *i);
        }
        Some(ParameterReference::Name(name)) => {
            let _ = err.setattr("parameter", name.to_string());
        }
        None => {
            let _ = err.setattr("parameter", py.None());
        }
    }
}

fn build_serialization_pyerr(
    py: Python<'_>,
    err: PyErr,
    location: &Option<ParameterReference>,
    cause: Option<PyErr>,
) -> PyErr {
    if let Some(cause) = cause {
        err.set_cause(py, Some(cause));
    }

    attach_serialization_location_attrs(py, err.value(py), location);
    err
}

impl From<DriverSerializationError> for PyErr {
    fn from(e: DriverSerializationError) -> PyErr {
        Python::attach(|py| {
            let location_as_string = format_serialization_location(&e.location);

            match e.kind {
                SerializationErrorKind::UnsupportedType { cql } => {
                    let message = if location_as_string.is_empty() {
                        format!("Unsupported CQL type: {cql}")
                    } else {
                        format!("Unsupported CQL type: {cql}{location_as_string}")
                    };

                    build_serialization_pyerr(
                        py,
                        UnsupportedTypeSerializationErrorPy::new_err(message),
                        &e.location,
                        None,
                    )
                }

                SerializationErrorKind::TypeMismatch { expected } => {
                    let message = if location_as_string.is_empty() {
                        format!("Type mismatch: expected {expected}")
                    } else {
                        format!("Type mismatch: expected {expected}{location_as_string}")
                    };

                    build_serialization_pyerr(
                        py,
                        TypeMismatchSerializationErrorPy::new_err(message),
                        &e.location,
                        None,
                    )
                }

                SerializationErrorKind::ValueOverflow => {
                    let message = if location_as_string.is_empty() {
                        "Value overflow during serialization".to_string()
                    } else {
                        format!("Value overflow during serialization{location_as_string}")
                    };

                    build_serialization_pyerr(
                        py,
                        ValueOverflowSerializationErrorPy::new_err(message),
                        &e.location,
                        None,
                    )
                }

                SerializationErrorKind::PythonInteropFailed { source } => {
                    let message = if location_as_string.is_empty() {
                        "Python interop failed".to_string()
                    } else {
                        format!("Python interop failed{location_as_string}")
                    };

                    build_serialization_pyerr(
                        py,
                        PySerializationFailedErrorPy::new_err(message),
                        &e.location,
                        Some(*source),
                    )
                }

                SerializationErrorKind::ScyllaSerializeFailed { source } => {
                    let base = source.to_string();
                    let message = if location_as_string.is_empty() {
                        base
                    } else {
                        format!("{base}{location_as_string}")
                    };

                    build_serialization_pyerr(
                        py,
                        SerializeFailedErrorPy::new_err(message),
                        &e.location,
                        None,
                    )
                }
            }
        })
    }
}

impl From<DriverSerializationError> for scylla::serialize::SerializationError {
    fn from(err: DriverSerializationError) -> Self {
        scylla::serialize::SerializationError::new(err)
    }
}

#[pymodule]
pub(crate) fn errors(py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add("ScyllaError", py.get_type::<ScyllaErrorPy>())?;
    module.add("RowIterationError", py.get_type::<RowIterationErrorPy>())?;
    module.add(
        "DeserializationError",
        py.get_type::<DeserializationErrorPy>(),
    )?;
    module.add(
        "UnsupportedTypeDeserializationError",
        py.get_type::<UnsupportedTypeDeserializationErrorPy>(),
    )?;
    module.add("DecodeFailedError", py.get_type::<DecodeFailedErrorPy>())?;
    module.add(
        "PyConversionFailedError",
        py.get_type::<PyConversionFailedErrorPy>(),
    )?;
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
    module.add(
        "StatementConfigError",
        py.get_type::<StatementConfigErrorPy>(),
    )?;
    module.add("BatchError", py.get_type::<BatchErrorPy>())?;
    module.add("SerializationError", py.get_type::<SerializationErrorPy>())?;
    module.add(
        "UnsupportedTypeSerializationError",
        py.get_type::<UnsupportedTypeSerializationErrorPy>(),
    )?;
    module.add(
        "TypeMismatchSerializationError",
        py.get_type::<TypeMismatchSerializationErrorPy>(),
    )?;
    module.add(
        "ValueOverflowSerializationError",
        py.get_type::<ValueOverflowSerializationErrorPy>(),
    )?;
    module.add(
        "SerializeFailedError",
        py.get_type::<SerializeFailedErrorPy>(),
    )?;
    module.add(
        "PySerializationFailedError",
        py.get_type::<PySerializationFailedErrorPy>(),
    )?;
    Ok(())
}
