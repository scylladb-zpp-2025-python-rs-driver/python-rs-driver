use pyo3::prelude::*;
use scylla::statement::{Consistency, SerialConsistency};
use scylla_cql::frame::Compression;

#[pyclass(name = "Consistency", eq, eq_int, frozen, from_py_object)]
#[derive(Clone, Copy, PartialEq)]
pub(crate) enum PyConsistency {
    Any,
    One,
    Two,
    Three,
    Quorum,
    All,
    LocalQuorum,
    EachQuorum,
    LocalOne,
    Serial,
    LocalSerial,
}

impl From<PyConsistency> for Consistency {
    fn from(value: PyConsistency) -> Self {
        match value {
            PyConsistency::Any => Self::Any,
            PyConsistency::One => Self::One,
            PyConsistency::Two => Self::Two,
            PyConsistency::Three => Self::Three,
            PyConsistency::Quorum => Self::Quorum,
            PyConsistency::All => Self::All,
            PyConsistency::LocalQuorum => Self::LocalQuorum,
            PyConsistency::EachQuorum => Self::EachQuorum,
            PyConsistency::LocalOne => Self::LocalOne,
            PyConsistency::Serial => Self::Serial,
            PyConsistency::LocalSerial => Self::LocalSerial,
        }
    }
}

impl From<Consistency> for PyConsistency {
    fn from(value: Consistency) -> Self {
        match value {
            Consistency::Any => Self::Any,
            Consistency::One => Self::One,
            Consistency::Two => Self::Two,
            Consistency::Three => Self::Three,
            Consistency::Quorum => Self::Quorum,
            Consistency::All => Self::All,
            Consistency::LocalQuorum => Self::LocalQuorum,
            Consistency::EachQuorum => Self::EachQuorum,
            Consistency::LocalOne => Self::LocalOne,
            Consistency::Serial => Self::Serial,
            Consistency::LocalSerial => Self::LocalSerial,
        }
    }
}

#[pyclass(name = "SerialConsistency", eq, eq_int, frozen, from_py_object)]
#[derive(Clone, Copy, PartialEq)]
pub(crate) enum PySerialConsistency {
    Serial,
    LocalSerial,
}

impl From<PySerialConsistency> for SerialConsistency {
    fn from(value: PySerialConsistency) -> Self {
        match value {
            PySerialConsistency::Serial => Self::Serial,
            PySerialConsistency::LocalSerial => Self::LocalSerial,
        }
    }
}

impl From<SerialConsistency> for PySerialConsistency {
    fn from(value: SerialConsistency) -> Self {
        match value {
            SerialConsistency::Serial => Self::Serial,
            SerialConsistency::LocalSerial => Self::LocalSerial,
        }
    }
}

#[pyclass(eq, eq_int, frozen, from_py_object, name = "Compression")]
#[derive(Clone, Copy, PartialEq, Debug)]
pub(crate) enum PyCompression {
    Lz4,
    Snappy,
}

impl From<PyCompression> for Compression {
    fn from(value: PyCompression) -> Self {
        match value {
            PyCompression::Lz4 => Self::Lz4,
            PyCompression::Snappy => Self::Snappy,
        }
    }
}

#[pymodule]
pub(crate) fn enums(_py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<PyConsistency>()?;
    module.add_class::<PySerialConsistency>()?;
    module.add_class::<PyCompression>()?;
    Ok(())
}
