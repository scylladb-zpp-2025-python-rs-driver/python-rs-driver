// src/errors.rs
use pyo3::PyErr;
use pyo3::create_exception;
use pyo3::exceptions::PyException;
use pyo3::prelude::*;
use pyo3::types::PyModule;

// Python exception classes
create_exception!(errors, ScyllaErrorPy, PyException);

create_exception!(errors, ExecutionErrorPy, ScyllaErrorPy);
create_exception!(errors, BadQueryErrorPy, ExecutionErrorPy);
create_exception!(errors, RuntimeErrorPy, ExecutionErrorPy);
create_exception!(errors, ConnectionErrorPy, ExecutionErrorPy);

create_exception!(errors, DeserializationErrorPy, ScyllaErrorPy);
create_exception!(errors, UnsupportedTypeErrorPy, DeserializationErrorPy);
create_exception!(errors, DecodeFailedErrorPy, DeserializationErrorPy);
create_exception!(errors, PyConversionFailedErrorPy, DeserializationErrorPy);
create_exception!(errors, WrongDeserializerErrorPy, DeserializationErrorPy);

// Policy: DriverError types are pure Rust and contain PyErr only as source
// in cases where the error originated from Python code (e.g. during extraction or user callbacks).
// Conversion to PyErr happens at the boundary (e.g. in #[pymethods] implementations)
// using the From<DriverError> for PyErr implementation, which maps each DriverError variant to
// an appropriate Python exception class and attaches any relevant information as attributes or causes.

/* Rust errors */
#[derive(Debug)]
#[allow(dead_code)] // we will have more variants here in the future
pub(crate) enum DriverError {
    Execution(DriverExecutionError),
    Deserialization(DriverDeserializationError),
    // Serialization(DriverSerializationError),
}

impl From<DriverError> for PyErr {
    fn from(e: DriverError) -> PyErr {
        match e {
            DriverError::Execution(e) => e.into(),
            DriverError::Deserialization(e) => e.into(),
        }
    }
}

/* Deserialization errors */

/// Errors that can occur during deserialization of CQL values into Python objects.
#[derive(Debug)]
pub struct DriverDeserializationError {
    pub kind: DeserializationErrorKind,
    pub location: DeserializationErrorLocation,
}

/// Structured information about where in the data the deserialization error occurred,
/// to provide better context in error messages and for debugging.
#[derive(Debug, Clone, Default)]
pub struct DeserializationErrorLocation {
    pub row: Option<usize>,
    pub column: Option<ColumnReference>,
    pub inner: Vec<InnerSegment>,
}

/// Represents a reference to a column, which can be by name or by index.
#[derive(Debug, Clone)]
pub enum ColumnReference {
    Name(String),
    Index(usize),
}

/// Represents a segment in the path to the value that failed to deserialize, for nested structures.
#[derive(Debug, Clone)]
pub enum InnerSegment {
    ListIndex(usize),
    MapIndex(usize),
    TupleIndex(usize),
    UdtField(String),
    VectorIndex(usize),
}

#[derive(Debug)]
pub enum DeserializationErrorKind {
    /// The CQL type is not supported by the deserializer
    /// (e.g. an unknown custom type, or a new type added in Scylla that we haven't implemented yet).
    UnsupportedType { cql: String },
    /// An error occurred during deserialization in the scylla_cql crate.
    ScyllaDecodeFailed {
        source: Box<scylla_cql::deserialize::DeserializationError>,
    },
    /// An error occurred during conversion to a Python object
    /// (e.g. invalid UTF-8, unsupported type for Python conversion, etc.).
    Python { source: Box<pyo3::PyErr> },
    /// Driver invariant violated: a deserializer was called for a mismatched ColumnType.
    /// This indicates a bug in our dispatch logic.
    WrongDeserializer { expected: &'static str, got: String },
}

impl DriverDeserializationError {
    /* Constructors */

    #[must_use]
    pub fn unsupported_type(cql: impl Into<String>) -> Self {
        Self {
            kind: DeserializationErrorKind::UnsupportedType { cql: cql.into() },
            location: DeserializationErrorLocation::default(),
        }
    }

    #[must_use]
    pub fn scylla(source: scylla_cql::deserialize::DeserializationError) -> Self {
        Self {
            kind: DeserializationErrorKind::ScyllaDecodeFailed {
                source: Box::new(source),
            },
            location: DeserializationErrorLocation::default(),
        }
    }

    #[must_use]
    pub fn python(source: pyo3::PyErr) -> Self {
        Self {
            kind: DeserializationErrorKind::Python {
                source: Box::new(source),
            },
            location: DeserializationErrorLocation::default(),
        }
    }

    #[must_use]
    pub fn wrong_deserializer(expected: &'static str, got: String) -> Self {
        Self {
            kind: DeserializationErrorKind::WrongDeserializer { expected, got },
            location: DeserializationErrorLocation::default(),
        }
    }

    /* Row and column setters */

    #[must_use]
    pub fn at_row(mut self, row: usize) -> Self {
        self.location.row = Some(row);
        self
    }

    #[must_use]
    pub fn at_column_name(mut self, name: impl Into<String>) -> Self {
        self.location.column = Some(ColumnReference::Name(name.into()));
        self
    }

    #[must_use]
    pub fn at_column_index(mut self, index: usize) -> Self {
        self.location.column = Some(ColumnReference::Index(index));
        self
    }

    /* Inner path pushers (nesting) */

    #[must_use]
    pub fn in_list_index(mut self, index: usize) -> Self {
        self.location.inner.push(InnerSegment::ListIndex(index));
        self
    }

    #[must_use]
    pub fn in_map_index(mut self, index: usize) -> Self {
        self.location.inner.push(InnerSegment::MapIndex(index));
        self
    }

    #[must_use]
    pub fn in_tuple_index(mut self, index: usize) -> Self {
        self.location.inner.push(InnerSegment::TupleIndex(index));
        self
    }

    #[must_use]
    pub fn in_udt_field(mut self, field: impl Into<String>) -> Self {
        self.location
            .inner
            .push(InnerSegment::UdtField(field.into()));
        self
    }

    #[must_use]
    pub fn in_vector_index(mut self, index: usize) -> Self {
        self.location.inner.push(InnerSegment::VectorIndex(index));
        self
    }
}

/// Helper function to format the location information into a human-readable string for error messages.
fn format_location(loc: &DeserializationErrorLocation) -> String {
    let mut parts: Vec<String> = Vec::new();

    if let Some(row) = loc.row {
        parts.push(format!("row={row}"));
    }

    if let Some(col) = &loc.column {
        match col {
            ColumnReference::Name(name) => parts.push(format!("column={name}")),
            ColumnReference::Index(i) => parts.push(format!("column_index={i}")),
        }
    }

    for seg in &loc.inner {
        let s = match seg {
            InnerSegment::ListIndex(i) => format!("list[{i}]"),
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

/// Attaches location attributes to the given Python exception instance
/// based on the provided `DeserializationErrorLocation`.
fn attach_location_attrs(
    py: Python<'_>,
    err: &Bound<'_, pyo3::PyAny>, // The exception instance we're attaching attributes to
    loc: &DeserializationErrorLocation,
) {
    // Row
    if let Some(row) = loc.row {
        let _ = err.setattr("row", row);
    } else {
        let _ = err.setattr("row", py.None());
    }

    // Column
    match &loc.column {
        Some(ColumnReference::Name(name)) => {
            let _ = err.setattr("column_name", name.as_str());
            let _ = err.setattr("column_index", py.None());
        }
        Some(ColumnReference::Index(i)) => {
            let _ = err.setattr("column_index", *i);
            let _ = err.setattr("column_name", py.None());
        }
        None => {
            let _ = err.setattr("column_name", py.None());
            let _ = err.setattr("column_index", py.None());
        }
    }

    // Inner path - we convert the inner path segments into a list of tuples for better structure in Python
    let inner_list = pyo3::types::PyList::empty(py);
    for seg in &loc.inner {
        let item = match seg {
            InnerSegment::ListIndex(i) => ("list", *i).into_pyobject(py).unwrap().into_any(),
            InnerSegment::MapIndex(i) => ("map", *i).into_pyobject(py).unwrap().into_any(),
            InnerSegment::TupleIndex(i) => ("tuple", *i).into_pyobject(py).unwrap().into_any(),
            InnerSegment::UdtField(f) => ("udt_field", f.as_str())
                .into_pyobject(py)
                .unwrap()
                .into_any(),
            InnerSegment::VectorIndex(i) => ("vector", *i).into_pyobject(py).unwrap().into_any(),
        };
        let _ = inner_list.append(item);
    }
    let _ = err.setattr("inner_path", inner_list);
}

impl From<DriverDeserializationError> for PyErr {
    fn from(e: DriverDeserializationError) -> PyErr {
        Python::attach(|py| {
            let location_as_string = format_location(&e.location);

            match e.kind {
                DeserializationErrorKind::UnsupportedType { cql } => {
                    let message = if location_as_string.is_empty() {
                        cql
                    } else {
                        format!("{cql}{location_as_string}")
                    };

                    let err = UnsupportedTypeErrorPy::new_err(message);

                    // Set location attributes
                    if let Ok(inst) = err.value(py).cast::<pyo3::PyAny>() {
                        attach_location_attrs(py, inst, &e.location);
                    }
                    err
                }

                DeserializationErrorKind::ScyllaDecodeFailed { source } => {
                    // We stringify the original error because scylla_cql::deserialize::DeserializationError
                    // doesn't implement Send + Sync, so we can't attach it as a cause in the PyErr.
                    // Instead, we include its message in our custom error and attach the location info as attributes.
                    // We could consider changing this to an enum-based error in the future if we want to preserve
                    // more structured information from the original error.
                    let base = source.to_string();
                    let message = if location_as_string.is_empty() {
                        base
                    } else {
                        format!("{base}{location_as_string}")
                    };

                    let err = DecodeFailedErrorPy::new_err(message);

                    // Set location attributes
                    if let Ok(inst) = err.value(py).cast::<pyo3::PyAny>() {
                        attach_location_attrs(py, inst, &e.location);
                    }
                    err
                }

                DeserializationErrorKind::Python { source } => {
                    let message = if location_as_string.is_empty() {
                        "Python conversion failed".to_string()
                    } else {
                        format!("Python conversion failed{location_as_string}")
                    };

                    let err = PyConversionFailedErrorPy::new_err(message);

                    // Attach original python exception as cause
                    err.set_cause(py, Some(*source));

                    // Set location attributes on the new error instance
                    if let Ok(inst) = err.value(py).cast::<pyo3::PyAny>() {
                        attach_location_attrs(py, inst, &e.location);
                    }
                    err
                }

                DeserializationErrorKind::WrongDeserializer { expected, got } => {
                    let message = if location_as_string.is_empty() {
                        format!("Wrong deserializer: expected {expected}, got {got}")
                    } else {
                        format!(
                            "Wrong deserializer: expected {expected}, got {got}{location_as_string}"
                        )
                    };
                    let err = WrongDeserializerErrorPy::new_err(message);

                    // Set location attributes
                    if let Ok(inst) = err.value(py).cast::<pyo3::PyAny>() {
                        attach_location_attrs(py, inst, &e.location);
                    }
                    err
                }
            }
        })
    }
}

// #[derive(Debug)]
// pub enum DriverExecutionError {
//     BadQuery(BadQueryError),
//     Connect(ConnectError),
//     Runtime(RuntimeError),
// }

// #[derive(Debug)]
// pub enum BadQueryError {
//     /// Session.execute(...) got a request of unsupported Python type.
//     InvalidRequestType(InvalidRequestType),

//     /// Session.prepare(...) got a statement of unsupported Python type.
//     InvalidStatementType(InvalidStatementType),

//     /// Building statement config failed due to invalid user input (timeout etc.)
//     InvalidTimeout(InvalidTimeout),

//     /// Parse/validate contact points failed due to user input issues.
//     InvalidContactPoints(InvalidContactPoints),

//     /// Scylla prepare failed due to invalid CQL / invalid request.
//     PrepareFailed(PrepareFailed),

//     /// Bad user input when configuring a statement (profiles, consistency, page size etc.)
//     ConfigureStatement(ConfigureStatementBadQuery),

//     /// Bad user input when building an execution profile.
//     BuildExecutionProfile(BuildExecutionProfileBadQuery),

//     /// Bad user input when building session config (port type/range, auth options etc.)
//     BuildSessionConfig(BuildSessionConfigBadQuery),
// }

// #[derive(Debug)]
// pub struct InvalidRequestType {
//     pub got_type: String,            // e.g. "list", "Foo", ...
//     pub expected: &'static str,      // e.g. "Statement | PreparedStatement | str"
//     pub source: Option<pyo3::PyErr>, // if the failure originated in extract/cast
// }

// #[derive(Debug)]
// pub struct InvalidStatementType {
//     pub got_type: String,
//     pub expected: &'static str, // e.g. "Statement | str"
//     pub source: Option<pyo3::PyErr>,
// }

// /// Timeout provided by user is <= 0, NaN, infinite, or cannot be parsed.
// #[derive(Debug)]
// pub struct InvalidTimeout {
//     pub timeout_repr: Option<String>, // optionally store repr/value as string
//     pub reason: TimeoutInvalidReason,
//     pub source: Option<pyo3::PyErr>,
// }

// #[derive(Debug)]
// pub enum TimeoutInvalidReason {
//     NonFinite,
//     NonPositive,
//     WrongType,
// }

// /// Covers cases like:
// /// - contact_points is a single string (explicitly rejected)
// /// - contact_points not iterable
// /// - element not a string / not utf-8
// #[derive(Debug)]
// pub struct InvalidContactPoints {
//     pub reason: ContactPointsInvalidReason,
//     pub source: Option<pyo3::PyErr>,
// }

// #[derive(Debug)]
// pub enum ContactPointsInvalidReason {
//     NotASequence,
//     SingleStringRejected,
//     ElementNotString {
//         index: usize,
//         got_type: Option<String>,
//     },
//     ElementNotUtf8 {
//         index: usize,
//     },
//     EmptyList,
//     AddrParse {
//         index: usize,
//         source: AddrParseError,
//     },
// }

// /// Scylla prepare failed due to invalid CQL, invalid keyspace/table, etc.
// #[derive(Debug)]
// pub struct PrepareFailed {
//     pub cql: String,
//     pub source: PrepareFailedSource,
// }

// #[derive(Debug)]
// pub enum PrepareFailedSource {
//     PyErr(pyo3::PyErr), // if this came from Python-side validation unexpectedly
//     Scylla(String),     // replace String with concrete scylla error later
// }

// /// “Statement configuration” rejected a parameter (consistency, page size, tracing, etc.)
// #[derive(Debug)]
// pub struct ConfigureStatementBadQuery {
//     pub param: &'static str,         // e.g. "consistency", "page_size", "tracing"
//     pub reason: String,              // short machine-ish reason
//     pub source: Option<pyo3::PyErr>, // typical for extraction failures
// }

// /// Execution profile build rejected user input.
// #[derive(Debug)]
// pub struct BuildExecutionProfileBadQuery {
//     pub field: &'static str, // e.g. "load_balancing", "retry_policy"
//     pub reason: String,
//     pub source: Option<pyo3::PyErr>,
// }

// /// Session config build rejected user input (port, auth provider, ssl context, etc.)
// #[derive(Debug)]
// pub struct BuildSessionConfigBadQuery {
//     pub field: &'static str, // e.g. "port", "auth", "ssl"
//     pub reason: String,
//     pub source: Option<pyo3::PyErr>,
// }

// #[derive(Debug)]
// pub enum ConnectError {
//     /// Session::connect(...) failed.
//     ConnectFailed(ConnectFailed),

//     /// Failed to build scylla SessionConfig from Python input.
//     BuildSessionConfig(BuildSessionConfigFailed),

//     /// Parsing contact points failed (may also be classed as BadQuery; your call).
//     ParseContactPoints(ParseContactPointsFailed),
// }

// #[derive(Debug)]
// pub struct ConnectFailed {
//     pub contact_points: Vec<String>,
//     pub port: u16,
//     pub source: ConnectFailedSource,
// }

// #[derive(Debug)]
// pub enum ConnectFailedSource {
//     Scylla(String), // replace with concrete scylla connect error
//     Io(std::io::Error),
//     PyErr(pyo3::PyErr), // if connect triggers Python callbacks; rare
// }

// #[derive(Debug)]
// pub struct BuildSessionConfigFailed {
//     pub contact_points: Option<Vec<String>>,
//     pub port: Option<u16>,
//     pub source: BuildSessionConfigSource,
// }

// #[derive(Debug)]
// pub enum BuildSessionConfigSource {
//     PyErr(pyo3::PyErr),
//     Internal(String),
// }

// #[derive(Debug)]
// pub struct ParseContactPointsFailed {
//     pub input_type: Option<String>, // if you want to store what the Python type was
//     pub source: ParseContactPointsSource,
// }

// #[derive(Debug)]
// pub enum ParseContactPointsSource {
//     PyErr(pyo3::PyErr),
//     BadValue(String),
// }

// #[derive(Debug)]
// pub enum RuntimeError {
//     ExecuteUnpaged(ExecuteUnpagedRuntime),
//     QueryUnpaged(QueryUnpagedRuntime),
//     SpawnJoin(SpawnJoinRuntime),
// }

// #[derive(Debug)]
// pub struct ExecuteUnpagedRuntime {
//     pub cql: Option<String>,
//     pub request_type: Option<String>, // e.g. "Statement" / "PreparedStatement"
//     pub source: ExecuteRuntimeSource,
// }

// #[derive(Debug)]
// pub enum ExecuteRuntimeSource {
//     Scylla(String),     // replace with concrete query error type
//     PyErr(pyo3::PyErr), // if Python callback used during execution; rare
// }

// #[derive(Debug)]
// pub struct QueryUnpagedRuntime {
//     pub cql: Option<String>,
//     pub request_type: Option<String>,
//     pub source: QueryRuntimeSource,
// }

// #[derive(Debug)]
// pub enum QueryRuntimeSource {
//     Scylla(String), // replace with concrete query error type
//     PyErr(pyo3::PyErr),
// }

// #[derive(Debug)]
// pub struct SpawnJoinRuntime {
//     pub task: &'static str, // "execute_unpaged" / "query_unpaged" etc.
//     pub source: tokio::task::JoinError,
// }

// impl From<DriverExecutionError> for PyErr {
//     fn from(e: DriverExecutionError) -> PyErr {
//         Python::attach(|py| {
//             let msg = format!("{:?}", e);
//             RuntimeErrorPy::new_err(msg)
//         })
//     }
// }

#[derive(Debug)]
pub struct DriverExecutionError {
    pub kind: ExecutionErrorKind,
    pub op: ExecutionOp,
    pub ctx: Box<ExecutionContext>,
}

#[derive(Debug)]
pub enum ExecutionSource {
    PyErr(pyo3::PyErr),
    RustErr(Box<dyn std::error::Error + Send + Sync>),
}

#[derive(Debug)]
pub enum ExecutionErrorKind {
    /// CQL / query related errors (syntax, invalid request, prepare failures, etc.)
    BadQuery { source: Option<ExecutionSource> },

    /// Failed to establish a session or connect to cluster.
    Connect { source: Option<ExecutionSource> },

    /// Runtime failures during request execution (timeouts, unavailable, protocol errors, join errors, etc.)
    Runtime { source: Option<ExecutionSource> },

    /// Internal invariant / bug in the driver binding layer.
    Internal { message: String },
}

#[derive(Debug)]
pub enum ExecutionOp {
    Connect,
    Prepare,
    ExecuteUnpaged,
    QueryUnpaged,
    SpawnJoin,             // tokio join error / runtime join
    BuildSessionConfig,    // building session config from Python input
    ParseContactPoints,    // parsing contact points from Python input
    ConfigureStatement,    // configuring statement from Python input
    BuildExecutionProfile, // building execution profile from Python input
}

#[derive(Debug)]
pub struct ExecutionContext {
    /// Human-readable short message
    pub message: Option<String>,

    /// The actual CQL statement being executed (if applicable)
    pub cql: Option<String>,

    /// Request type seen at boundary
    pub request_type: Option<String>,

    /// For connect errors
    pub contact_points: Option<Vec<String>>,
    pub port: Option<u16>,
}

impl DriverExecutionError {
    pub fn with_cql(mut self, cql: impl Into<String>) -> Self {
        self.ctx.cql = Some(cql.into());
        self
    }

    pub fn with_request_type(mut self, ty: impl Into<String>) -> Self {
        self.ctx.request_type = Some(ty.into());
        self
    }

    pub fn with_contact_points(mut self, contact_points: Vec<String>, port: u16) -> Self {
        self.ctx.contact_points = Some(contact_points);
        self.ctx.port = Some(port);
        self
    }

    pub fn bad_query(
        op: ExecutionOp,
        source: Option<ExecutionSource>,
        msg: impl Into<String>,
    ) -> Self {
        Self {
            kind: ExecutionErrorKind::BadQuery { source },
            op,
            ctx: Box::new(ExecutionContext {
                message: Some(msg.into()),
                cql: None,
                request_type: None,
                contact_points: None,
                port: None,
            }),
        }
    }

    pub fn runtime(
        op: ExecutionOp,
        source: Option<ExecutionSource>,
        msg: impl Into<String>,
    ) -> Self {
        Self {
            kind: ExecutionErrorKind::Runtime { source },
            op,
            ctx: Box::new(ExecutionContext {
                message: Some(msg.into()),
                cql: None,
                request_type: None,
                contact_points: None,
                port: None,
            }),
        }
    }

    pub fn connect(
        op: ExecutionOp,
        source: Option<ExecutionSource>,
        msg: impl Into<String>,
    ) -> Self {
        Self {
            kind: ExecutionErrorKind::Connect { source },
            op,
            ctx: Box::new(ExecutionContext {
                message: Some(msg.into()),
                cql: None,
                request_type: None,
                contact_points: None,
                port: None,
            }),
        }
    }

    pub fn internal(op: ExecutionOp, message: impl Into<String>) -> Self {
        Self {
            kind: ExecutionErrorKind::Internal {
                message: message.into(),
            },
            op,
            ctx: Box::new(ExecutionContext {
                message: None,
                cql: None,
                request_type: None,
                contact_points: None,
                port: None,
            }),
        }
    }
}

impl From<DriverExecutionError> for PyErr {
    fn from(e: DriverExecutionError) -> PyErr {
        Python::attach(|py| {
            let msg = format_execution_error_message(&e);

            match e.kind {
                ExecutionErrorKind::BadQuery { source } => {
                    let outer = BadQueryErrorPy::new_err(msg);

                    if let Some(ExecutionSource::PyErr(cause)) = source {
                        outer.set_cause(py, Some(cause));
                    }

                    outer
                }
                ExecutionErrorKind::Connect { source } => {
                    let outer = ConnectionErrorPy::new_err(msg);
                    if let Some(ExecutionSource::PyErr(cause)) = source {
                        outer.set_cause(py, Some(cause));
                    }
                    outer
                }
                ExecutionErrorKind::Runtime { source } => {
                    let outer = RuntimeErrorPy::new_err(msg);
                    if let Some(ExecutionSource::PyErr(cause)) = source {
                        outer.set_cause(py, Some(cause));
                    }
                    outer
                }
                ExecutionErrorKind::Internal { .. } => RuntimeErrorPy::new_err(msg),
            }
        })
    }
}

fn op_name(op: &ExecutionOp) -> &'static str {
    match op {
        ExecutionOp::Connect => "connect",
        ExecutionOp::Prepare => "prepare",
        ExecutionOp::ExecuteUnpaged => "execute_unpaged",
        ExecutionOp::QueryUnpaged => "query_unpaged",
        ExecutionOp::SpawnJoin => "spawn_join",
        ExecutionOp::BuildSessionConfig => "build_session_config",
        ExecutionOp::ParseContactPoints => "parse_contact_points",
        ExecutionOp::ConfigureStatement => "configure_statement",
        ExecutionOp::BuildExecutionProfile => "build_execution_profile",
    }
}

fn format_execution_error_message(e: &DriverExecutionError) -> String {
    let mut parts: Vec<String> = Vec::new();

    // Operation where error occurred
    parts.push(format!("op={}", op_name(&e.op)));

    // Message
    if let Some(m) = &e.ctx.message {
        parts.push(m.clone());
    }

    // Request type
    if let Some(t) = &e.ctx.request_type {
        parts.push(format!("request_type={t}"));
    }

    // CQL statement
    if let Some(cql) = &e.ctx.cql {
        parts.push(format!("cql={cql}"));
    }

    // Connect-specific context
    if let Some(cp) = &e.ctx.contact_points {
        parts.push(format!("contact_points={cp:?}"));
    }
    if let Some(port) = e.ctx.port {
        parts.push(format!("port={port}"));
    }

    // Kind-specific details
    match &e.kind {
        ExecutionErrorKind::BadQuery { source }
        | ExecutionErrorKind::Connect { source }
        | ExecutionErrorKind::Runtime { source } => {
            if let Some(ExecutionSource::RustErr(err)) = source.as_ref() {
                parts.push(format!("cause={err}"));
            }
            // PyErr cause is attached separately in From<DriverExecutionError> for PyErr
        }

        ExecutionErrorKind::Internal { message } => {
            parts.push(format!("internal={message}"));
        }
    }

    parts.join(" | ")
}

#[pymodule]
pub(crate) fn errors(_py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add("ScyllaError", _py.get_type::<ScyllaErrorPy>())?;
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
    module.add(
        "WrongDeserializerError",
        _py.get_type::<WrongDeserializerErrorPy>(),
    )?;
    Ok(())
}
