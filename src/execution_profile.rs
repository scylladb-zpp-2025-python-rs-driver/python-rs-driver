use pyo3::prelude::*;
use scylla::client;
use std::time::Duration;

use crate::enums::{PyConsistency, PySerialConsistency};
use crate::errors::DriverStatementConfigError;

#[pyclass(frozen, from_py_object)]
#[derive(Clone)]
pub(crate) struct ExecutionProfile {
    pub(crate) _inner: client::execution_profile::ExecutionProfile,
}

#[pymethods]
impl ExecutionProfile {
    #[new]
    #[pyo3(signature = (
        timeout=30.0,
        consistency=PyConsistency::LocalQuorum,
        serial_consistency=PySerialConsistency::LocalSerial,
    ))]
    pub(crate) fn new(
        timeout: Option<f64>,
        consistency: PyConsistency,
        serial_consistency: Option<PySerialConsistency>,
    ) -> Result<Self, DriverStatementConfigError> {
        let mut profile_builder = client::execution_profile::ExecutionProfile::builder();

        if let Some(secs) = timeout
            && (!secs.is_finite() || secs <= 0.0)
        {
            return Err(DriverStatementConfigError::InvalidRequestTimeout { value: secs });
        }

        if let Some(secs) = timeout {
            let duration = Duration::try_from_secs_f64(secs)
                .map_err(|_| DriverStatementConfigError::request_timeout_conversion_failed(secs))?;

            profile_builder = profile_builder.request_timeout(Some(duration));
        }

        profile_builder = profile_builder.consistency(consistency.to_rust());

        profile_builder =
            profile_builder.serial_consistency(serial_consistency.map(|sc| sc.to_rust()));

        Ok(ExecutionProfile {
            _inner: profile_builder.build(),
        })
    }

    pub(crate) fn get_request_timeout(&self) -> Option<f64> {
        self._inner.get_request_timeout().map(|d| d.as_secs_f64())
    }

    pub(crate) fn get_consistency(&self) -> PyConsistency {
        PyConsistency::to_python(self._inner.get_consistency())
    }

    pub(crate) fn get_serial_consistency(&self) -> Option<PySerialConsistency> {
        self._inner
            .get_serial_consistency()
            .map(PySerialConsistency::to_python)
    }
}

#[pymodule]
pub(crate) fn execution_profile(_py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<ExecutionProfile>()?;
    Ok(())
}
