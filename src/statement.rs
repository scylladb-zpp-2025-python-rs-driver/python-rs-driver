use pyo3::exceptions::PyValueError;
use pyo3::types::{PyFloat, PyString};
use pyo3::{IntoPyObjectExt, prelude::*};
use scylla::statement::prepared;
use scylla::statement::unprepared;
use std::time::Duration;

use crate::enums::{Consistency, SerialConsistency};
use crate::execution_profile::ExecutionProfile;
use crate::types::UnsetType;

#[pyclass(frozen)]
pub(crate) struct PreparedStatement {
    pub(crate) _inner: prepared::PreparedStatement,
    // Because `get_serial_consistency` in the Rust driver returns `Option<SerialConsistency>`,
    // it cannot represent the `Unset` state. Therefore, the Python-rs driver must distinguish
    // between `Unset` and `None` in a different way. To preserve this distinction, an additional
    // flag `is_serial_consistency_set` is required.
    is_serial_consistency_set: bool,
}

impl PreparedStatement {
    pub(crate) fn new(
        _inner: prepared::PreparedStatement,
        is_serial_consistency_set: bool,
    ) -> Self {
        Self {
            _inner,
            is_serial_consistency_set,
        }
    }
}

#[pymethods]
impl PreparedStatement {
    fn with_execution_profile(&self, profile: ExecutionProfile) -> PreparedStatement {
        let mut p = self._inner.clone();
        p.set_execution_profile_handle(Some(profile._inner.into_handle()));
        PreparedStatement::new(p, self.is_serial_consistency_set)
    }

    fn without_execution_profile(&self) -> PreparedStatement {
        let mut p = self._inner.clone();
        p.set_execution_profile_handle(None);
        PreparedStatement::new(p, self.is_serial_consistency_set)
    }

    #[getter]
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
        PreparedStatement::new(p, self.is_serial_consistency_set)
    }

    fn without_consistency(&self) -> PreparedStatement {
        let mut p = self._inner.clone();
        p.unset_consistency();
        PreparedStatement::new(p, self.is_serial_consistency_set)
    }

    #[getter]
    fn get_consistency(&self) -> Option<Consistency> {
        self._inner.get_consistency().map(Consistency::to_python)
    }

    fn with_serial_consistency(&self, sc: Option<SerialConsistency>) -> PreparedStatement {
        let mut p = self._inner.clone();
        p.set_serial_consistency(sc.map(|sc| sc.to_rust()));
        PreparedStatement::new(p, true)
    }

    fn without_serial_consistency(&self) -> PreparedStatement {
        let mut p = self._inner.clone();
        p.unset_serial_consistency();
        PreparedStatement::new(p, false)
    }

    #[getter]
    fn get_serial_consistency(&self, py: Python) -> PyResult<Py<PyAny>> {
        if !self.is_serial_consistency_set {
            return UnsetType::get_instance(py).into_py_any(py);
        }
        match self._inner.get_serial_consistency() {
            Some(sc) => SerialConsistency::to_python(sc).into_py_any(py),
            None => Ok(py.None()),
        }
    }

    fn with_request_timeout(&self, timeout: Option<f64>) -> PyResult<PreparedStatement> {
        if let Some(secs) = timeout
            && (!secs.is_finite() || secs <= 0.0)
        {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "timeout must be a positive, finite number (in seconds)",
            ));
        }

        let timeout = match timeout {
            None => Duration::MAX,
            Some(secs) => Duration::try_from_secs_f64(secs)
                .map_err(|e| PyValueError::new_err(e.to_string()))?,
        };

        let mut p = self._inner.clone();
        p.set_request_timeout(Some(timeout));
        Ok(PreparedStatement::new(p, self.is_serial_consistency_set))
    }

    fn without_request_timeout(&self) -> PreparedStatement {
        let mut p = self._inner.clone();
        p.set_request_timeout(None);
        PreparedStatement::new(p, self.is_serial_consistency_set)
    }

    #[getter]
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
        PreparedStatement::new(p, self.is_serial_consistency_set)
    }

    #[getter]
    fn get_page_size(&self) -> i32 {
        self._inner.get_page_size()
    }
}

#[pyclass(frozen)]
pub(crate) struct Statement {
    pub(crate) _inner: unprepared::Statement,
    // Because `get_serial_consistency` in the Rust driver returns `Option<SerialConsistency>`,
    // it cannot represent the `Unset` state. Therefore, the Python-rs driver must distinguish
    // between `Unset` and `None` in a different way. To preserve this distinction, an additional
    // flag `is_serial_consistency_set` is required.
    is_serial_consistency_set: bool,
}

#[pymethods]
impl Statement {
    #[new]
    fn new(query_str: String) -> PyResult<Statement> {
        let s = unprepared::Statement::from(query_str);
        Ok(Statement {
            _inner: s,
            is_serial_consistency_set: false,
        })
    }

    #[getter]
    fn contents<'py>(&self, py: Python<'py>) -> Bound<'py, PyString> {
        PyString::new(py, &self._inner.contents)
    }

    fn with_execution_profile(&self, profile: ExecutionProfile) -> Statement {
        let mut s = self._inner.clone();
        s.set_execution_profile_handle(Some(profile._inner.into_handle()));
        Statement {
            _inner: s,
            is_serial_consistency_set: self.is_serial_consistency_set,
        }
    }

    fn without_execution_profile(&self) -> Statement {
        let mut s = self._inner.clone();
        s.set_execution_profile_handle(None);
        Statement {
            _inner: s,
            is_serial_consistency_set: self.is_serial_consistency_set,
        }
    }

    #[getter]
    fn get_execution_profile(&self) -> Option<ExecutionProfile> {
        self._inner
            .get_execution_profile_handle()
            .map(|h| ExecutionProfile {
                _inner: h.to_profile(),
            })
    }

    fn with_consistency(&self, c: Consistency) -> Statement {
        let mut s = self._inner.clone();
        s.set_consistency(c.to_rust());
        Statement {
            _inner: s,
            is_serial_consistency_set: self.is_serial_consistency_set,
        }
    }

    fn without_consistency(&self) -> Statement {
        let mut s = self._inner.clone();
        s.unset_consistency();
        Statement {
            _inner: s,
            is_serial_consistency_set: self.is_serial_consistency_set,
        }
    }

    #[getter]
    fn get_consistency(&self) -> Option<Consistency> {
        self._inner.get_consistency().map(Consistency::to_python)
    }

    fn with_serial_consistency(&self, sc: Option<SerialConsistency>) -> Statement {
        let mut s = self._inner.clone();
        s.set_serial_consistency(sc.map(|sc| sc.to_rust()));
        Statement {
            _inner: s,
            is_serial_consistency_set: true,
        }
    }

    fn without_serial_consistency(&self) -> Statement {
        let mut s = self._inner.clone();
        s.unset_serial_consistency();
        Statement {
            _inner: s,
            is_serial_consistency_set: false,
        }
    }

    #[getter]
    fn get_serial_consistency(&self, py: Python) -> PyResult<Py<PyAny>> {
        if !self.is_serial_consistency_set {
            return UnsetType::get_instance(py).into_py_any(py);
        }
        match self._inner.get_serial_consistency() {
            Some(sc) => SerialConsistency::to_python(sc).into_py_any(py),
            None => Ok(py.None()),
        }
    }

    fn with_request_timeout(&self, timeout: Option<f64>) -> PyResult<Statement> {
        if let Some(secs) = timeout
            && (!secs.is_finite() || secs <= 0.0)
        {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "timeout must be a positive, finite number (in seconds)",
            ));
        }

        let timeout = match timeout {
            None => Duration::MAX,
            Some(secs) => Duration::try_from_secs_f64(secs)
                .map_err(|e| PyValueError::new_err(e.to_string()))?,
        };

        let mut s = self._inner.clone();
        s.set_request_timeout(Some(timeout));
        Ok(Statement {
            _inner: s,
            is_serial_consistency_set: self.is_serial_consistency_set,
        })
    }

    fn without_request_timeout(&self) -> Statement {
        let mut s = self._inner.clone();
        s.set_request_timeout(None);
        Statement {
            _inner: s,
            is_serial_consistency_set: self.is_serial_consistency_set,
        }
    }

    #[getter]
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
            is_serial_consistency_set: self.is_serial_consistency_set,
        }
    }

    #[getter]
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
