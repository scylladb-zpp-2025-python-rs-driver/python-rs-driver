use pyo3::types::PyFloat;
use pyo3::{IntoPyObjectExt, prelude::*};
use scylla::statement::prepared;
use std::time::Duration;

use crate::enums::{Consistency, SerialConsistency};
use crate::execution_profile::ExecutionProfile;
use crate::types::UnsetType;

#[pyclass(frozen)]
pub(crate) struct PreparedStatement {
    pub(crate) _inner: prepared::PreparedStatement,
}

#[pymethods]
impl PreparedStatement {
    fn with_execution_profile(&self, profile: ExecutionProfile) -> PreparedStatement {
        let mut p = self._inner.clone();
        p.set_execution_profile_handle(Some(profile._inner.into_handle()));
        PreparedStatement { _inner: p }
    }

    fn without_execution_profile(&self) -> PreparedStatement {
        let mut p = self._inner.clone();
        p.set_execution_profile_handle(None);
        PreparedStatement { _inner: p }
    }

    fn get_execution_profile(&self) -> Option<ExecutionProfile> {
        self._inner
            .get_execution_profile_handle()
            .map(|h| ExecutionProfile {
                _inner: h.to_profile(),
            })
    }

    fn with_consistency(&self, c: Consistency) -> PreparedStatement {
        let mut p = self._inner.clone();
        p.set_consistency(c.to_rust());
        PreparedStatement { _inner: p }
    }

    fn without_consistency(&self) -> PreparedStatement {
        let mut p = self._inner.clone();
        p.unset_consistency();
        PreparedStatement { _inner: p }
    }

    fn get_consistency(&self) -> Option<Consistency> {
        self._inner.get_consistency().map(Consistency::to_python)
    }

    fn with_serial_consistency(&self, sc: Option<SerialConsistency>) -> PreparedStatement {
        let mut p = self._inner.clone();
        p.set_serial_consistency(sc.map(|sc| sc.to_rust()));
        PreparedStatement { _inner: p }
    }

    fn without_serial_consistency(&self) -> PreparedStatement {
        let mut p = self._inner.clone();
        p.unset_serial_consistency();
        PreparedStatement { _inner: p }
    }

    fn get_serial_consistency(&self) -> Option<SerialConsistency> {
        // TODO: implement returning Unset like in get_request_timeout
        self._inner
            .get_serial_consistency()
            .map(SerialConsistency::to_python)
    }

    fn with_request_timeout(&self, timeout: Option<f64>) -> PyResult<PreparedStatement> {
        if let Some(secs) = timeout
            && (!secs.is_finite() || secs <= 0.0)
        {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "timeout must be a positive, finite number (in seconds)",
            ));
        }

        let mut p = self._inner.clone();
        p.set_request_timeout(Some(timeout.map_or(Duration::MAX, Duration::from_secs_f64)));
        Ok(PreparedStatement { _inner: p })
    }

    fn without_request_timeout(&self) -> PreparedStatement {
        let mut p = self._inner.clone();
        p.set_request_timeout(None);
        PreparedStatement { _inner: p }
    }

    fn get_request_timeout(&self) -> PyResult<Py<PyAny>> {
        match self._inner.get_request_timeout() {
            Some(t) if t == Duration::MAX => Ok(Python::attach(|py| py.None())),
            Some(t) => Python::attach(|py| PyFloat::new(py, t.as_secs_f64()).into_py_any(py)),
            None => Python::attach(|py| UnsetType::get_instance(py).unwrap().into_py_any(py)),
        }
    }
}

#[pymodule]
pub(crate) fn statement(_py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<PreparedStatement>()?;
    Ok(())
}
