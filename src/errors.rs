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

#[allow(dead_code)]
#[derive(Debug)]
pub(crate) enum DriverError {
    Execution(DriverExecutionError),
    Deserialization(DriverDeserializationError),
}

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
    todo: String,
}

impl DriverExecutionError {
    pub fn new(msg: impl Into<String>) -> Self {
        Self { todo: msg.into() }
    }
}

impl From<DriverExecutionError> for PyErr {
    fn from(e: DriverExecutionError) -> PyErr {
        ExecutionErrorPy::new_err(e.todo)
    }
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
