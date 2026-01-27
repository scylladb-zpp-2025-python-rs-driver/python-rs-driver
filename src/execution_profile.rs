use pyo3::prelude::*;
use scylla::client;
use std::time::Duration;

use crate::enums::{Consistency, SerialConsistency};
use crate::errors::{DriverExecutionError, ExecutionOp, ExecutionSource};

#[pyclass(frozen)]
#[derive(Clone)]
pub(crate) struct ExecutionProfile {
    pub(crate) _inner: client::execution_profile::ExecutionProfile,
}

#[pymethods]
impl ExecutionProfile {
    #[new]
    #[pyo3(signature = (
        timeout=30.0,
        consistency=Consistency::LocalQuorum,
        serial_consistency=SerialConsistency::LocalSerial,
    ))]
    pub(crate) fn new(
        timeout: Option<f64>,
        consistency: Consistency,
        serial_consistency: Option<SerialConsistency>,
    ) -> Result<Self, DriverExecutionError> {
        let mut profile_builder = client::execution_profile::ExecutionProfile::builder();

        if let Some(secs) = timeout
            && (!secs.is_finite() || secs <= 0.0)
        {
            let cause = pyo3::exceptions::PyValueError::new_err(
                "timeout must be a positive, finite number (in seconds)",
            );
            return Err(DriverExecutionError::bad_query(
                ExecutionOp::BuildExecutionProfile,
                Some(ExecutionSource::PyErr(cause)),
                "invalid timeout value",
            ));
        }

        profile_builder = profile_builder.request_timeout(timeout.map(Duration::from_secs_f64));

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

    pub(crate) fn get_consistency(&self) -> Consistency {
        Consistency::to_python(self._inner.get_consistency())
    }

    pub(crate) fn get_serial_consistency(&self) -> Option<SerialConsistency> {
        self._inner
            .get_serial_consistency()
            .map(SerialConsistency::to_python)
    }
}

#[pymodule]
pub(crate) fn execution_profile(_py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<ExecutionProfile>()?;
    Ok(())
}
