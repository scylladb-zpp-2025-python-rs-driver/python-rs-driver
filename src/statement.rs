use pyo3::types::{PyFloat, PyString};
use pyo3::{IntoPyObjectExt, prelude::*};
use scylla::statement::prepared;
use scylla::statement::unprepared;
use std::sync::Arc;
use std::time::Duration;

use crate::enums::{Consistency, SerialConsistency};
use crate::execution_profile::ExecutionProfile;
use crate::policies::load_balancing::PyLoadBalancingPolicy;
use crate::types::UnsetType;

#[pyclass(frozen)]
pub(crate) struct PreparedStatement {
    pub(crate) _inner: prepared::PreparedStatement,
    pub(crate) _load_balancing_policy: Option<PyLoadBalancingPolicy>,
    pub(crate) _execution_profile: Option<Py<ExecutionProfile>>,
}

#[pymethods]
impl PreparedStatement {
    fn with_execution_profile(&self, profile: Py<ExecutionProfile>) -> PreparedStatement {
        let mut p = self._inner.clone();
        let inner = profile.get()._inner.as_ref().clone();
        p.set_execution_profile_handle(Some(inner.into_handle()));
        PreparedStatement {
            _inner: p,
            _load_balancing_policy: self._load_balancing_policy.clone(),
            _execution_profile: Some(profile),
        }
    }

    fn without_execution_profile(&self) -> PreparedStatement {
        let mut p = self._inner.clone();
        p.set_execution_profile_handle(None);
        PreparedStatement {
            _inner: p,
            _load_balancing_policy: self._load_balancing_policy.clone(),
            _execution_profile: None,
        }
    }

    fn get_execution_profile(&self) -> Option<Py<ExecutionProfile>> {
        self._execution_profile.clone()
    }

    fn with_load_balancing_policy(&self, policy: Py<PyAny>) -> PreparedStatement {
        let lbp = PyLoadBalancingPolicy { _inner: policy };
        let mut p = self._inner.clone();
        p.set_load_balancing_policy(Some(Arc::new(lbp.clone())));
        PreparedStatement {
            _inner: p,
            _load_balancing_policy: Some(lbp),
            _execution_profile: self._execution_profile.clone(),
        }
    }

    fn without_load_balancing_policy(&self) -> PreparedStatement {
        let mut p = self._inner.clone();
        p.set_load_balancing_policy(None);
        PreparedStatement {
            _inner: p,
            _load_balancing_policy: None,
            _execution_profile: self._execution_profile.clone(),
        }
    }

    fn get_load_balancing_policy(&self) -> Option<PyLoadBalancingPolicy> {
        self._load_balancing_policy.clone()
    }

    fn with_consistency(&self, c: Consistency) -> PreparedStatement {
        let mut p = self._inner.clone();
        p.set_consistency(c.to_rust());
        PreparedStatement {
            _inner: p,
            _load_balancing_policy: self._load_balancing_policy.clone(),
            _execution_profile: self._execution_profile.clone(),
        }
    }

    fn without_consistency(&self) -> PreparedStatement {
        let mut p = self._inner.clone();
        p.unset_consistency();
        PreparedStatement {
            _inner: p,
            _load_balancing_policy: self._load_balancing_policy.clone(),
            _execution_profile: self._execution_profile.clone(),
        }
    }

    fn get_consistency(&self) -> Option<Consistency> {
        self._inner.get_consistency().map(Consistency::to_python)
    }

    fn with_serial_consistency(&self, sc: Option<SerialConsistency>) -> PreparedStatement {
        let mut p = self._inner.clone();
        p.set_serial_consistency(sc.map(|sc| sc.to_rust()));
        PreparedStatement {
            _inner: p,
            _load_balancing_policy: self._load_balancing_policy.clone(),
            _execution_profile: self._execution_profile.clone(),
        }
    }

    fn without_serial_consistency(&self) -> PreparedStatement {
        let mut p = self._inner.clone();
        p.unset_serial_consistency();
        PreparedStatement {
            _inner: p,
            _load_balancing_policy: self._load_balancing_policy.clone(),
            _execution_profile: self._execution_profile.clone(),
        }
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
        Ok(PreparedStatement {
            _inner: p,
            _load_balancing_policy: self._load_balancing_policy.clone(),
            _execution_profile: self._execution_profile.clone(),
        })
    }

    fn without_request_timeout(&self) -> PreparedStatement {
        let mut p = self._inner.clone();
        p.set_request_timeout(None);
        PreparedStatement {
            _inner: p,
            _load_balancing_policy: self._load_balancing_policy.clone(),
            _execution_profile: self._execution_profile.clone(),
        }
    }

    fn get_request_timeout(&self) -> PyResult<Py<PyAny>> {
        match self._inner.get_request_timeout() {
            Some(t) if t == Duration::MAX => Ok(Python::attach(|py| py.None())),
            Some(t) => Python::attach(|py| PyFloat::new(py, t.as_secs_f64()).into_py_any(py)),
            None => Python::attach(|py| UnsetType::get_instance(py).into_py_any(py)),
        }
    }

    fn with_page_size(&self, page_size: i32) -> PreparedStatement {
        let mut p = self._inner.clone();
        p.set_page_size(page_size);
        PreparedStatement {
            _inner: p,
            _load_balancing_policy: self._load_balancing_policy.clone(),
            _execution_profile: self._execution_profile.clone(),
        }
    }

    fn get_page_size(&self) -> i32 {
        self._inner.get_page_size()
    }
}

#[pyclass(frozen)]
pub(crate) struct Statement {
    pub(crate) _inner: unprepared::Statement,
    pub(crate) _load_balancing_policy: Option<PyLoadBalancingPolicy>,
    pub(crate) _execution_profile: Option<Py<ExecutionProfile>>,
}

#[pymethods]
impl Statement {
    #[new]
    fn new(query_str: String) -> PyResult<Statement> {
        let s = unprepared::Statement::from(query_str);
        Ok(Statement {
            _inner: s,
            _load_balancing_policy: None,
            _execution_profile: None,
        })
    }

    #[getter]
    fn contents<'py>(&self, py: Python<'py>) -> Bound<'py, PyString> {
        PyString::new(py, &self._inner.contents)
    }

    fn with_execution_profile(&self, profile: Py<ExecutionProfile>) -> Statement {
        let mut s = self._inner.clone();
        let inner = profile.get()._inner.as_ref().clone();
        s.set_execution_profile_handle(Some(inner.into_handle()));
        Statement {
            _inner: s,
            _load_balancing_policy: self._load_balancing_policy.clone(),
            _execution_profile: Some(profile),
        }
    }

    fn without_execution_profile(&self) -> Statement {
        let mut s = self._inner.clone();
        s.set_execution_profile_handle(None);
        Statement {
            _inner: s,
            _load_balancing_policy: self._load_balancing_policy.clone(),
            _execution_profile: None,
        }
    }

    fn get_execution_profile(&self) -> Option<Py<ExecutionProfile>> {
        self._execution_profile.clone()
    }

    fn with_load_balancing_policy(&self, policy: Py<PyAny>) -> Statement {
        let lbp = PyLoadBalancingPolicy { _inner: policy };
        let mut s = self._inner.clone();
        s.set_load_balancing_policy(Some(Arc::new(lbp.clone())));
        Statement {
            _inner: s,
            _load_balancing_policy: Some(lbp),
            _execution_profile: self._execution_profile.clone(),
        }
    }

    fn without_load_balancing_policy(&self) -> Statement {
        let mut s = self._inner.clone();
        s.set_load_balancing_policy(None);
        Statement {
            _inner: s,
            _load_balancing_policy: None,
            _execution_profile: self._execution_profile.clone(),
        }
    }

    fn get_load_balancing_policy(&self) -> Option<PyLoadBalancingPolicy> {
        self._load_balancing_policy.clone()
    }

    fn with_consistency(&self, c: Consistency) -> Statement {
        let mut s = self._inner.clone();
        s.set_consistency(c.to_rust());
        Statement {
            _inner: s,
            _load_balancing_policy: self._load_balancing_policy.clone(),
            _execution_profile: self._execution_profile.clone(),
        }
    }

    fn without_consistency(&self) -> Statement {
        let mut s = self._inner.clone();
        s.unset_consistency();
        Statement {
            _inner: s,
            _load_balancing_policy: self._load_balancing_policy.clone(),
            _execution_profile: self._execution_profile.clone(),
        }
    }

    fn get_consistency(&self) -> Option<Consistency> {
        self._inner.get_consistency().map(Consistency::to_python)
    }

    fn with_serial_consistency(&self, sc: Option<SerialConsistency>) -> Statement {
        let mut s = self._inner.clone();
        s.set_serial_consistency(sc.map(|sc| sc.to_rust()));
        Statement {
            _inner: s,
            _load_balancing_policy: self._load_balancing_policy.clone(),
            _execution_profile: self._execution_profile.clone(),
        }
    }

    fn without_serial_consistency(&self) -> Statement {
        let mut s = self._inner.clone();
        s.unset_serial_consistency();
        Statement {
            _inner: s,
            _load_balancing_policy: self._load_balancing_policy.clone(),
            _execution_profile: self._execution_profile.clone(),
        }
    }

    fn get_serial_consistency(&self) -> Option<SerialConsistency> {
        // TODO: implement returning Unset like in get_request_timeout
        self._inner
            .get_serial_consistency()
            .map(SerialConsistency::to_python)
    }

    fn with_request_timeout(&self, timeout: Option<f64>) -> PyResult<Statement> {
        if let Some(secs) = timeout
            && (!secs.is_finite() || secs <= 0.0)
        {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "timeout must be a positive, finite number (in seconds)",
            ));
        }

        let mut s = self._inner.clone();
        s.set_request_timeout(Some(timeout.map_or(Duration::MAX, Duration::from_secs_f64)));
        Ok(Statement {
            _inner: s,
            _load_balancing_policy: self._load_balancing_policy.clone(),
            _execution_profile: self._execution_profile.clone(),
        })
    }

    fn without_request_timeout(&self) -> Statement {
        let mut s = self._inner.clone();
        s.set_request_timeout(None);
        Statement {
            _inner: s,
            _load_balancing_policy: self._load_balancing_policy.clone(),
            _execution_profile: self._execution_profile.clone(),
        }
    }

    fn get_request_timeout(&self) -> PyResult<Py<PyAny>> {
        match self._inner.get_request_timeout() {
            Some(t) if t == Duration::MAX => Ok(Python::attach(|py| py.None())),
            Some(t) => Python::attach(|py| PyFloat::new(py, t.as_secs_f64()).into_py_any(py)),
            None => Python::attach(|py| UnsetType::get_instance(py).into_py_any(py)),
        }
    }

    fn with_page_size(&self, page_size: i32) -> Statement {
        let mut s = self._inner.clone();
        s.set_page_size(page_size);
        Statement {
            _inner: s,
            _load_balancing_policy: self._load_balancing_policy.clone(),
            _execution_profile: self._execution_profile.clone(),
        }
    }

    fn get_page_size(&self) -> i32 {
        self._inner.get_page_size()
    }
}

#[pymodule]
pub(crate) fn statement(_py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<PreparedStatement>()?;
    module.add_class::<Statement>()?;
    Ok(())
}
