use pyo3::prelude::*;
use scylla::statement;

#[pyclass(eq, eq_int, frozen, from_py_object)]
#[derive(Clone, Copy, PartialEq)]
pub(crate) enum Consistency {
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

impl From<Consistency> for statement::Consistency {
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

impl From<statement::Consistency> for Consistency {
    fn from(value: statement::Consistency) -> Self {
        match value {
            statement::Consistency::Any => Self::Any,
            statement::Consistency::One => Self::One,
            statement::Consistency::Two => Self::Two,
            statement::Consistency::Three => Self::Three,
            statement::Consistency::Quorum => Self::Quorum,
            statement::Consistency::All => Self::All,
            statement::Consistency::LocalQuorum => Self::LocalQuorum,
            statement::Consistency::EachQuorum => Self::EachQuorum,
            statement::Consistency::LocalOne => Self::LocalOne,
            statement::Consistency::Serial => Self::Serial,
            statement::Consistency::LocalSerial => Self::LocalSerial,
        }
    }
}

#[pyclass(eq, eq_int, frozen, from_py_object)]
#[derive(Clone, Copy, PartialEq)]
pub(crate) enum SerialConsistency {
    Serial,
    LocalSerial,
}

impl From<SerialConsistency> for statement::SerialConsistency {
    fn from(value: SerialConsistency) -> Self {
        match value {
            SerialConsistency::Serial => Self::Serial,
            SerialConsistency::LocalSerial => Self::LocalSerial,
        }
    }
}

impl From<statement::SerialConsistency> for SerialConsistency {
    fn from(value: statement::SerialConsistency) -> Self {
        match value {
            statement::SerialConsistency::Serial => Self::Serial,
            statement::SerialConsistency::LocalSerial => Self::LocalSerial,
        }
    }
}

#[pymodule]
pub(crate) fn enums(_py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<Consistency>()?;
    module.add_class::<SerialConsistency>()?;
    Ok(())
}
