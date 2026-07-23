use crate::enums::{PyConsistency, PySerialConsistency};
use crate::errors::DriverStatementConfigError;
use crate::policies::retry::policies::py_any_to_arc_retry_policy;
use pyo3::prelude::*;
use scylla::client::execution_profile::ExecutionProfile;
use std::time::Duration;

#[pyclass(name = "ExecutionProfile", frozen, from_py_object)]
#[derive(Clone)]
pub(crate) struct PyExecutionProfile {
    pub(crate) _inner: ExecutionProfile,
    pub(crate) retry_policy: Option<Py<PyAny>>,
}

#[pymethods]
impl PyExecutionProfile {
    #[new]
    #[pyo3(signature = (
        timeout=30.0,
        consistency=PyConsistency::LocalQuorum,
        serial_consistency=PySerialConsistency::LocalSerial,
        retry_policy=None,
    ))]
    pub(crate) fn new(
        timeout: Option<f64>,
        consistency: PyConsistency,
        serial_consistency: Option<PySerialConsistency>,
        retry_policy: Option<Py<PyAny>>,
        py: Python,
    ) -> Result<Self, DriverStatementConfigError> {
        let mut profile_builder = ExecutionProfile::builder();

        if let Some(secs) = timeout {
            let duration = Duration::try_from_secs_f64(secs)
                .map_err(|_| DriverStatementConfigError::invalid_request_timeout(secs))?;

            profile_builder = profile_builder.request_timeout(Some(duration));
        }

        profile_builder = profile_builder.consistency(consistency.into());

        profile_builder =
            profile_builder.serial_consistency(serial_consistency.map(|sc| sc.into()));

        if let Some(ref rp) = retry_policy {
            profile_builder = profile_builder.retry_policy(py_any_to_arc_retry_policy(rp, py));
        }

        Ok(PyExecutionProfile {
            _inner: profile_builder.build(),
            retry_policy,
        })
    }

    #[getter]
    pub(crate) fn get_request_timeout(&self) -> Option<f64> {
        self._inner.get_request_timeout().map(|d| d.as_secs_f64())
    }

    #[getter]
    pub(crate) fn get_consistency(&self) -> PyConsistency {
        PyConsistency::from(self._inner.get_consistency())
    }

    #[getter]
    pub(crate) fn get_serial_consistency(&self) -> Option<PySerialConsistency> {
        self._inner
            .get_serial_consistency()
            .map(PySerialConsistency::from)
    }

    #[getter]
    pub(crate) fn get_retry_policy(&self) -> Option<Py<PyAny>> {
        self.retry_policy.clone()
    }
}

#[pymodule]
pub(crate) fn execution_profile(_py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<PyExecutionProfile>()?;
    Ok(())
}
