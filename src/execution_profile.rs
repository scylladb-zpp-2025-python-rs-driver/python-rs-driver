use pyo3::prelude::*;
use scylla::client;
use std::{sync::Arc, time::Duration};

use crate::{
    enums::{Consistency, SerialConsistency},
    policies::load_balancing::PyLoadBalancingPolicy,
};

#[pyclass(frozen)]
#[derive(Clone)]
pub(crate) struct ExecutionProfile {
    pub(crate) _inner: Arc<client::execution_profile::ExecutionProfile>,
    pub(crate) _load_balancing_policy: Option<PyLoadBalancingPolicy>,
}

#[pymethods]
impl ExecutionProfile {
    #[new]
    #[pyo3(signature = (
        timeout=30.0,
        consistency=Consistency::LocalQuorum,
        serial_consistency=SerialConsistency::LocalSerial,
        policy=None,
    ))]
    pub(crate) fn new(
        timeout: Option<f64>,
        consistency: Consistency,
        serial_consistency: Option<SerialConsistency>,
        policy: Option<Py<PyAny>>,
    ) -> PyResult<Self> {
        let mut profile_builder = client::execution_profile::ExecutionProfile::builder();

        if let Some(secs) = timeout
            && (!secs.is_finite() || secs <= 0.0)
        {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "timeout must be a positive, finite number (in seconds)",
            ));
        }

        profile_builder = profile_builder.request_timeout(timeout.map(Duration::from_secs_f64));

        profile_builder = profile_builder.consistency(consistency.to_rust());

        profile_builder =
            profile_builder.serial_consistency(serial_consistency.map(|sc| sc.to_rust()));

        let stored_policy = if let Some(policy) = policy {
            let lbp = PyLoadBalancingPolicy { _inner: policy };
            let stored = lbp.clone();
            profile_builder = profile_builder.load_balancing_policy(Arc::new(lbp));
            Some(stored)
        } else {
            None
        };

        Ok(ExecutionProfile {
            _inner: Arc::new(profile_builder.build()),
            _load_balancing_policy: stored_policy,
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

    pub(crate) fn get_load_balancing_policy(&self) -> Option<PyLoadBalancingPolicy> {
        self._load_balancing_policy.clone()
    }
}

#[pymodule]
pub(crate) fn execution_profile(_py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<ExecutionProfile>()?;
    Ok(())
}
