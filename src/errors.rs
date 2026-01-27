// src/errors.rs
use pyo3::PyErr;
use pyo3::create_exception;
use pyo3::exceptions::PyException;
use pyo3::prelude::*;
use pyo3::types::PyModule;

// Python exception classes
create_exception!(errors, ScyllaError, PyException);

create_exception!(errors, ExecutionErrorPy, ScyllaError);
create_exception!(errors, BadQueryErrorPy, ExecutionErrorPy);
create_exception!(errors, RuntimeErrorPy, ExecutionErrorPy);
create_exception!(errors, ConnectionErrorPy, ExecutionErrorPy);

create_exception!(errors, DeserializationErrorPy, ScyllaError);
create_exception!(errors, UnsupportedTypeErrorPy, DeserializationErrorPy);
create_exception!(errors, DecodeFailedErrorPy, DeserializationErrorPy);
create_exception!(errors, PyConversionFailedErrorPy, DeserializationErrorPy);
create_exception!(errors, InternalErrorPy, DeserializationErrorPy);

// Rust errors

// #[derive(Debug)]
// pub(crate) enum DriverError {
//     Execution(DriverExecutionError),
//     Deserialization(DriverDeserializationError),
//     Serialization(DriverSerializationError),
// }

#[derive(Debug)]
pub struct DriverDeserializationError {
    pub kind: DeserializationErrorKind,
    pub path: Vec<DeserializationErrorSegment>,
}

#[derive(Debug)]
pub enum DeserializationErrorKind {
    UnsupportedType {
        cql: String,
    },
    ScyllaDecodeFailed {
        source: scylla_cql::deserialize::DeserializationError,
    },
    Python {
        source: pyo3::PyErr,
    },
    Internal {
        message: String,
    },
}

impl DriverDeserializationError {
    pub fn with_context(mut self, segment: DeserializationErrorSegment) -> Self {
        self.path.insert(0, segment); // prepend for natural reading
        self
    }

    pub fn scylla(source: scylla_cql::deserialize::DeserializationError) -> Self {
        Self {
            kind: DeserializationErrorKind::ScyllaDecodeFailed { source },
            path: Vec::new(),
        }
    }

    pub fn python(source: pyo3::PyErr) -> Self {
        Self {
            kind: DeserializationErrorKind::Python { source },
            path: Vec::new(),
        }
    }

    pub fn unsupported_type(cql: impl Into<String>) -> Self {
        Self {
            kind: DeserializationErrorKind::UnsupportedType { cql: cql.into() },
            path: Vec::new(),
        }
    }

    pub fn internal(msg: impl Into<String>) -> Self {
        Self {
            kind: DeserializationErrorKind::Internal {
                message: msg.into(),
            },
            path: Vec::new(),
        }
    }
}

#[derive(Debug, Clone)]
pub enum DeserializationErrorSegment {
    Column(String),
    Row(usize),
    ListIndex(usize),
    MapIndex(usize),
    TupleIndex(usize),
    UdtField(String),
    VectorIndex(usize),
}

/// Format the error path into a string for error messages
fn format_path(path: &[DeserializationErrorSegment]) -> String {
    if path.is_empty() {
        return String::new();
    }
    let mut parts = Vec::with_capacity(path.len());
    for seg in path {
        let s = match seg {
            DeserializationErrorSegment::Column(name) => format!("column={name}"),
            DeserializationErrorSegment::Row(i) => format!("row={i}"),
            DeserializationErrorSegment::ListIndex(i) => format!("list[{i}]"),
            DeserializationErrorSegment::MapIndex(i) => format!("map[{i}]"),
            DeserializationErrorSegment::TupleIndex(i) => format!("tuple[{i}]"),
            DeserializationErrorSegment::UdtField(f) => format!("udt.{f}"),
            DeserializationErrorSegment::VectorIndex(i) => format!("vector[{i}]"),
        };
        parts.push(s);
    }
    format!(" ({})", parts.join(" -> "))
}

/// Format a PyErr into a string "TypeName: message"
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

impl From<DriverDeserializationError> for PyErr {
    fn from(e: DriverDeserializationError) -> PyErr {
        let ctx = format_path(&e.path);

        Python::attach(|py| {
            match e.kind {
                DeserializationErrorKind::UnsupportedType { cql } => {
                    UnsupportedTypeErrorPy::new_err(format!("{cql}{ctx}"))
                }
                DeserializationErrorKind::ScyllaDecodeFailed { source } => {
                    DecodeFailedErrorPy::new_err(format!("{}{ctx}", source))
                }
                DeserializationErrorKind::Python { source } => {
                    // Create custom Driver exception
                    let base = format_pyerr(&source); // e.g. "TypeError: unhashable type: 'list'"
                    let msg = if ctx.is_empty() {
                        format!("Python conversion failed: {base}")
                    } else {
                        format!("Python conversion failed: {base}{ctx}")
                    };
                    let new_err = PyConversionFailedErrorPy::new_err(msg);

                    // Attach the original PyErr as the cause
                    new_err.set_cause(py, Some(source));

                    new_err
                }
                DeserializationErrorKind::Internal { message } => {
                    InternalErrorPy::new_err(format!("{message}{ctx}"))
                }
            }
        })
    }
}

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
