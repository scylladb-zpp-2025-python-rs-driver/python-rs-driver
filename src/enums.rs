use pyo3::prelude::*;
use scylla::statement::{Consistency, SerialConsistency};

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

impl PyConsistency {
    pub(crate) fn to_rust(self) -> Consistency {
        match self {
            PyConsistency::Any => Consistency::Any,
            PyConsistency::One => Consistency::One,
            PyConsistency::Two => Consistency::Two,
            PyConsistency::Three => Consistency::Three,
            PyConsistency::Quorum => Consistency::Quorum,
            PyConsistency::All => Consistency::All,
            PyConsistency::LocalQuorum => Consistency::LocalQuorum,
            PyConsistency::EachQuorum => Consistency::EachQuorum,
            PyConsistency::LocalOne => Consistency::LocalOne,
            PyConsistency::Serial => Consistency::Serial,
            PyConsistency::LocalSerial => Consistency::LocalSerial,
        }
    }

    pub(crate) fn to_python(consistency: Consistency) -> Self {
        match consistency {
            Consistency::Any => PyConsistency::Any,
            Consistency::One => PyConsistency::One,
            Consistency::Two => PyConsistency::Two,
            Consistency::Three => PyConsistency::Three,
            Consistency::Quorum => PyConsistency::Quorum,
            Consistency::All => PyConsistency::All,
            Consistency::LocalQuorum => PyConsistency::LocalQuorum,
            Consistency::EachQuorum => PyConsistency::EachQuorum,
            Consistency::LocalOne => PyConsistency::LocalOne,
            Consistency::Serial => PyConsistency::Serial,
            Consistency::LocalSerial => PyConsistency::LocalSerial,
        }
    }
}

#[pyclass(name = "SerialConsistency", eq, eq_int, frozen, from_py_object)]
#[derive(Clone, Copy, PartialEq)]
pub(crate) enum PySerialConsistency {
    Serial,
    LocalSerial,
}

impl PySerialConsistency {
    pub(crate) fn to_rust(self) -> SerialConsistency {
        match self {
            PySerialConsistency::Serial => SerialConsistency::Serial,
            PySerialConsistency::LocalSerial => SerialConsistency::LocalSerial,
        }
    }

    pub(crate) fn to_python(consistency: SerialConsistency) -> Self {
        match consistency {
            SerialConsistency::Serial => PySerialConsistency::Serial,
            SerialConsistency::LocalSerial => PySerialConsistency::LocalSerial,
        }
    }
}

#[pymodule]
pub(crate) fn enums(_py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<PyConsistency>()?;
    module.add_class::<PySerialConsistency>()?;
    Ok(())
}
