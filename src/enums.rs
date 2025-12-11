use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use scylla::statement;

#[pyclass(frozen)]
#[derive(Clone)]
pub(crate) struct Consistency {
    pub(crate) _inner: statement::Consistency,
}

#[pymethods]
impl Consistency {
    #[new]
    pub(crate) fn new(consistency: String) -> PyResult<Self> {
        let _inner = match consistency.as_str() {
            "Any" => statement::Consistency::Any,
            "One" => statement::Consistency::One,
            "Two" => statement::Consistency::Two,
            "Three" => statement::Consistency::Three,
            "Quorum" => statement::Consistency::Quorum,
            "All" => statement::Consistency::All,
            "LocalQuorum" => statement::Consistency::LocalQuorum,
            "EachQuorum" => statement::Consistency::EachQuorum,
            "LocalOne" => statement::Consistency::LocalOne,
            "Serial" => statement::Consistency::Serial,
            "LocalSerial" => statement::Consistency::LocalSerial,
            _ => {
                return Err(PyValueError::new_err(format!(
                    "Invalid consistency level: '{}'. Valid values are: Any, One, Two, Three, Quorum, All, LocalQuorum, EachQuorum, LocalOne, Serial, LocalSerial",
                    consistency
                )));
            }
        };
        Ok(Consistency { _inner })
    }

    fn __str__(&self) -> PyResult<String> {
        let consistency = match self._inner {
            statement::Consistency::Any => "Any",
            statement::Consistency::One => "One",
            statement::Consistency::Two => "Two",
            statement::Consistency::Three => "Three",
            statement::Consistency::Quorum => "Quorum",
            statement::Consistency::All => "All",
            statement::Consistency::LocalQuorum => "LocalQuorum",
            statement::Consistency::EachQuorum => "EachQuorum",
            statement::Consistency::LocalOne => "LocalOne",
            statement::Consistency::Serial => "Serial",
            statement::Consistency::LocalSerial => "LocalSerial",
        };
        Ok(consistency.to_string())
    }
}

#[pyclass(frozen)]
#[derive(Clone)]
pub(crate) struct SerialConsistency {
    pub(crate) _inner: statement::SerialConsistency,
}

#[pymethods]
impl SerialConsistency {
    #[new]
    pub(crate) fn new(consistency: String) -> PyResult<Self> {
        let _inner = match consistency.as_str() {
            "Serial" => statement::SerialConsistency::Serial,
            "LocalSerial" => statement::SerialConsistency::LocalSerial,
            _ => {
                return Err(PyValueError::new_err(format!(
                    "Invalid serial consistency level: '{}'. Valid values are: Serial, LocalSerial",
                    consistency
                )));
            }
        };
        Ok(SerialConsistency { _inner })
    }

    fn __str__(&self) -> PyResult<String> {
        let consistency = match self._inner {
            statement::SerialConsistency::Serial => "Serial",
            statement::SerialConsistency::LocalSerial => "LocalSerial",
        };
        Ok(consistency.to_string())
    }
}

#[pymodule]
pub(crate) fn enums(_py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<Consistency>()?;
    module.add_class::<SerialConsistency>()?;
    Ok(())
}
