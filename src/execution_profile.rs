use pyo3::prelude::*;
use scylla::client;
use std::time::Duration;

use crate::enums::{Consistency, SerialConsistency};

#[pyclass(frozen)]
#[derive(Clone)]
pub(crate) struct ExecutionProfile {
    pub(crate) _inner: client::execution_profile::ExecutionProfile,
}

#[pymethods]
impl ExecutionProfile {
    #[new]
    #[pyo3(signature = (timeout=None, consistency=None, serial_consistency=None))]
    pub(crate) fn new(
        timeout: Option<PyObject>,
        consistency: Option<PyObject>,
        serial_consistency: Option<PyObject>,
    ) -> PyResult<Self> {
        let mut profile_builder = client::execution_profile::ExecutionProfile::builder();

        if let Some(timeout) = timeout {
            let secs = Python::with_gil(|py| timeout.extract::<f64>(py))?;
            if !secs.is_finite() || secs <= 0.0 {
                return Err(pyo3::exceptions::PyValueError::new_err(
                    "timeout must be a positive, finite number (in seconds)",
                ));
            }
            let millis = (secs * 1000.0) as u64;
            profile_builder = profile_builder.request_timeout(Some(Duration::from_millis(millis)));
        }

        if let Some(consistency) = consistency {
            let consistency = Python::with_gil(|py| consistency.extract::<Consistency>(py))?;
            profile_builder = profile_builder.consistency(consistency._inner);
        }

        if let Some(serial_consistency) = serial_consistency {
            let serial_consistency =
                Python::with_gil(|py| serial_consistency.extract::<SerialConsistency>(py))?;
            profile_builder = profile_builder.serial_consistency(Some(serial_consistency._inner));
        }
        //add .load_balancing_policy after implemented
        //add .retry_policy after implemented
        //add .speculative_execution_policy() after implemented
        Ok(ExecutionProfile {
            _inner: profile_builder.build(),
        })
    }

    pub(crate) fn get_request_timeout(&self) -> Option<f64> {
        self._inner.get_request_timeout().map(|d| d.as_secs_f64())
    }

    pub(crate) fn get_consistency(&self) -> Consistency {
        Consistency {
            _inner: self._inner.get_consistency(),
        }
    }

    pub(crate) fn get_serial_consistency(&self) -> Option<SerialConsistency> {
        self._inner
            .get_serial_consistency()
            .map(|sc| SerialConsistency { _inner: sc })
    }
}

#[pymodule]
pub(crate) fn execution_profile(_py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<ExecutionProfile>()?;
    Ok(())
}
