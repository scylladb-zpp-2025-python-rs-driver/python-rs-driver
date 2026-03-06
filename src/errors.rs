// src/errors.rs
use pyo3::PyErr;
use pyo3::create_exception;
use pyo3::exceptions::PyException;
use pyo3::prelude::*;
use pyo3::types::PyModule;

// Python exception classes
create_exception!(errors, ScyllaErrorPy, PyException);

create_exception!(errors, DeserializationErrorPy, ScyllaErrorPy);
create_exception!(errors, UnsupportedTypeErrorPy, DeserializationErrorPy);
create_exception!(errors, DecodeFailedErrorPy, DeserializationErrorPy);
create_exception!(errors, PyConversionFailedErrorPy, DeserializationErrorPy);
create_exception!(errors, WrongDeserializerErrorPy, DeserializationErrorPy);

create_exception!(errors, ExecutionErrorPy, ScyllaErrorPy);
create_exception!(errors, ConnectionErrorPy, ExecutionErrorPy);
create_exception!(errors, SessionConfigErrorPy, ExecutionErrorPy);
create_exception!(errors, InvalidPortErrorPy, SessionConfigErrorPy);
create_exception!(errors, ContactPointsTypeErrorPy, SessionConfigErrorPy);
create_exception!(errors, ContactPointsLengthFailedPy, SessionConfigErrorPy);
create_exception!(errors, ContactPointAccessFailedPy, SessionConfigErrorPy);
create_exception!(errors, ContactPointTypeErrorPy, SessionConfigErrorPy);
create_exception!(errors, ContactPointConversionFailedPy, SessionConfigErrorPy);

// Policy: DriverError types are pure Rust and contain PyErr only as source
// in cases where the error originated from Python code (e.g. during extraction or user callbacks).
// Conversion to PyErr happens at the boundary (e.g. in #[pymethods] implementations)
// using the From<DriverError> for PyErr implementation, which maps each DriverError variant to
// an appropriate Python exception class and attaches any relevant information as attributes or causes.

/* Rust errors */

#[derive(Debug)]
#[allow(dead_code)] // we will have more variants here in the future
pub(crate) enum DriverError {
    Deserialization(DriverDeserializationError),
    Execution(DriverExecutionError),
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

/* Execution errors */

/// Errors that can occur during execution of queries, connection establishment, or session configuration.
#[derive(Debug)]
pub enum DriverExecutionError {
    ConnectionError(ConnectionError),
    SessionConfigError(SessionConfigError),
}

impl From<DriverExecutionError> for PyErr {
    fn from(e: DriverExecutionError) -> PyErr {
        match e {
            DriverExecutionError::ConnectionError(e) => e.into(),
            DriverExecutionError::SessionConfigError(e) => e.into(),
        }
    }
}

#[derive(Debug)]
pub enum ConnectionError {}

impl From<ConnectionError> for PyErr {
    fn from(_e: ConnectionError) -> PyErr {
        ConnectionErrorPy::new_err("Temporary ConnectionError") // Placeholder until we define the variants and their data
    }
}

#[derive(Debug)]
pub enum SessionConfigError {
    /// The provided port value is invalid (e.g. not an integer, or out of the valid range).
    InvalidPort { source: Box<PyErr> },
    /// The contact_points argument is of the wrong type (e.g. a string instead of a list).
    ContactPointsTypeError,
    /// Failed to determine the length of `contact_points`.
    ContactPointsLengthFailed { source: Box<PyErr> },
    /// Failed to access an item in the contact_points iterable at the given index.
    ContactPointAccessFailed { index: usize, source: Box<PyErr> },
    /// An item in the contact_points iterable is of the wrong type (e.g. not a string).
    ContactPointTypeError { index: usize, source: Box<PyErr> },
    /// Failed to convert an item in the contact_points iterable to a string (e.g. invalid UTF-8).
    ContactPointConversionFailed { index: usize, source: Box<PyErr> },
}

impl SessionConfigError {
    /* Constructors */

    #[must_use]
    pub fn invalid_port(source: PyErr) -> Self {
        Self::InvalidPort {
            source: Box::new(source),
        }
    }

    #[must_use]
    pub fn contact_points_type_error() -> Self {
        Self::ContactPointsTypeError
    }

    #[must_use]
    pub fn contact_points_length_failed(source: PyErr) -> Self {
        Self::ContactPointsLengthFailed {
            source: Box::new(source),
        }
    }

    #[must_use]
    pub fn contact_point_access_failed(index: usize, source: PyErr) -> Self {
        Self::ContactPointAccessFailed {
            index,
            source: Box::new(source),
        }
    }

    #[must_use]
    pub fn contact_point_type_error(index: usize, source: PyErr) -> Self {
        Self::ContactPointTypeError {
            index,
            source: Box::new(source),
        }
    }

    #[must_use]
    pub fn contact_point_conversion_failed(index: usize, source: PyErr) -> Self {
        Self::ContactPointConversionFailed {
            index,
            source: Box::new(source),
        }
    }
}

impl From<SessionConfigError> for PyErr {
    fn from(e: SessionConfigError) -> PyErr {
        Python::attach(|py| match e {
            SessionConfigError::InvalidPort { source } => {
                let message =
                    ("Invalid port value: expected an integer between 0 and 65535.").to_string();

                let err = InvalidPortErrorPy::new_err(message);

                err.set_cause(py, Some(*source));
                err
            }

            SessionConfigError::ContactPointsTypeError => ContactPointsTypeErrorPy::new_err(
                "contact_points should be a sequence of strings, not a string!",
            ),

            SessionConfigError::ContactPointsLengthFailed { source } => {
                let message = "failed to determine the length of contact_points".to_string();

                let err = ContactPointsLengthFailedPy::new_err(message);

                err.set_cause(py, Some(*source));
                err
            }

            SessionConfigError::ContactPointAccessFailed { index, source } => {
                let message = format!("Failed to access contact point at index {}", index);

                let err = ContactPointAccessFailedPy::new_err(message);

                err.set_cause(py, Some(*source));

                if let Ok(inst) = err.value(py).cast::<pyo3::PyAny>() {
                    let _ = inst.setattr("index", index);
                }
                err
            }

            SessionConfigError::ContactPointTypeError { index, source } => {
                let message = format!(
                    "Contact points should be strings! Invalid contact point at index {}",
                    index
                );

                let err = ContactPointTypeErrorPy::new_err(message);

                err.set_cause(py, Some(*source));

                if let Ok(inst) = err.value(py).cast::<pyo3::PyAny>() {
                    let _ = inst.setattr("index", index);
                }
                err
            }

            SessionConfigError::ContactPointConversionFailed { index, source } => {
                let message = format!(
                    "Failed to convert contact point at index {} to string",
                    index
                );

                let err = ContactPointConversionFailedPy::new_err(message);

                err.set_cause(py, Some(*source));

                if let Ok(inst) = err.value(py).cast::<pyo3::PyAny>() {
                    let _ = inst.setattr("index", index);
                }
                err
            }
        })
    }
}

#[pymodule]
pub(crate) fn errors(_py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add("ScyllaError", _py.get_type::<ScyllaErrorPy>())?;
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
    module.add("ExecutionError", _py.get_type::<ExecutionErrorPy>())?;
    module.add("ConnectionError", _py.get_type::<ConnectionErrorPy>())?;
    module.add("SessionConfigError", _py.get_type::<SessionConfigErrorPy>())?;
    module.add("InvalidPortError", _py.get_type::<InvalidPortErrorPy>())?;
    module.add(
        "ContactPointsTypeErrorError",
        _py.get_type::<ContactPointsTypeErrorPy>(),
    )?;
    module.add(
        "ContactPointsLengthFailedError",
        _py.get_type::<ContactPointsLengthFailedPy>(),
    )?;
    module.add(
        "ContactPointAccessFailedError",
        _py.get_type::<ContactPointAccessFailedPy>(),
    )?;
    module.add(
        "ContactPointTypeError",
        _py.get_type::<ContactPointTypeErrorPy>(),
    )?;
    module.add(
        "ContactPointConversionFailedError",
        _py.get_type::<ContactPointConversionFailedPy>(),
    )?;
    Ok(())
}
