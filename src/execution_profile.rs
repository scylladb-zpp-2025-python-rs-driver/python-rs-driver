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
        timeout: Option<f64>,
        consistency: Option<Consistency>,
        serial_consistency: Option<SerialConsistency>,
    ) -> PyResult<Self> {
        let mut profile_builder = client::execution_profile::ExecutionProfile::builder();

        if let Some(secs) = timeout {
            if !secs.is_finite() || secs <= 0.0 {
                return Err(pyo3::exceptions::PyValueError::new_err(
                    "timeout must be a positive, finite number (in seconds)",
                ));
            }
            profile_builder = profile_builder.request_timeout(Some(Duration::from_secs_f64(secs)));
        }

        if let Some(consistency) = consistency {
            profile_builder = profile_builder.consistency(consistency.to_scylla());
        }

        if let Some(serial_consistency) = serial_consistency {
            if serial_consistency == SerialConsistency::Unset {
                return Err(pyo3::exceptions::PyValueError::new_err(
                    "Serial consistency can't be set to SerialConsistency.Unset",
                ));
            }
            profile_builder = profile_builder.serial_consistency(serial_consistency.to_scylla());
        }

        Ok(ExecutionProfile {
            _inner: profile_builder.build(),
        })
    }

    pub(crate) fn get_request_timeout(&self) -> Option<f64> {
        self._inner.get_request_timeout().map(|d| d.as_secs_f64())
    }

    pub(crate) fn get_consistency(&self) -> Consistency {
        Consistency::from_scylla(self._inner.get_consistency())
    }

    pub(crate) fn get_serial_consistency(&self) -> SerialConsistency {
        SerialConsistency::from_scylla(self._inner.get_serial_consistency())
    }
}

#[pymodule]
pub(crate) fn execution_profile(_py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<ExecutionProfile>()?;
    Ok(())
}
