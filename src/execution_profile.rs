use pyo3::prelude::*;
use scylla::client;
use std::time::Duration;

use crate::enums::{PyConsistency, PySerialConsistency};
use crate::errors::DriverStatementConfigError;
use crate::load_balancing::PyLoadBalancingPolicy;

#[pyclass(frozen, from_py_object)]
#[derive(Clone)]
pub(crate) struct ExecutionProfile {
    pub(crate) _inner: client::execution_profile::ExecutionProfile,
    pub(crate) _load_balancing_policy: Option<Py<PyAny>>,
}

#[pymethods]
impl ExecutionProfile {
    #[new]
    #[pyo3(signature = (
        timeout=30.0,
        consistency=PyConsistency::LocalQuorum,
        serial_consistency=PySerialConsistency::LocalSerial,
        load_balancing_policy=None,
    ))]
    pub(crate) fn new(
        py: Python<'_>,
        timeout: Option<f64>,
        consistency: PyConsistency,
        serial_consistency: Option<PySerialConsistency>,
        load_balancing_policy: Option<Py<PyAny>>,
    ) -> Result<Self, DriverStatementConfigError> {
        let mut profile_builder = client::execution_profile::ExecutionProfile::builder();

        if let Some(secs) = timeout {
            let duration = Duration::try_from_secs_f64(secs)
                .map_err(|_| DriverStatementConfigError::invalid_request_timeout(secs))?;

            profile_builder = profile_builder.request_timeout(Some(duration));
        }

        profile_builder = profile_builder.consistency(consistency.into());

        profile_builder =
            profile_builder.serial_consistency(serial_consistency.map(|sc| sc.into()));

        if let Some(ref py_policy) = load_balancing_policy {
            let policy = py_policy.extract::<PyLoadBalancingPolicy>(py)?;
            profile_builder = profile_builder.load_balancing_policy(policy.into_inner());
        }

        Ok(ExecutionProfile {
            _inner: profile_builder.build(),
            _load_balancing_policy: load_balancing_policy,
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
    fn get_load_balancing_policy(&self) -> Option<Py<PyAny>> {
        self._load_balancing_policy.clone()
    }
}

#[pymodule]
pub(crate) fn execution_profile(_py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<ExecutionProfile>()?;
    Ok(())
}
