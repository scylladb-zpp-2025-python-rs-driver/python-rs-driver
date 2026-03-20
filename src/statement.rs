use pyo3::prelude::*;
use pyo3::types::{PyFloat, PyString};
use scylla::statement::prepared;
use scylla::statement::unprepared;
use std::time::Duration;

use crate::enums::{Consistency, SerialConsistency};
use crate::errors::StatementConfigError;
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

    fn with_request_timeout(
        &self,
        timeout: Option<f64>,
    ) -> Result<PreparedStatement, StatementConfigError> {
        if let Some(secs) = timeout
            && (!secs.is_finite() || secs <= 0.0)
        {
            return Err(StatementConfigError::InvalidRequestTimeout { value: secs });
        }

        let timeout = match timeout {
            None => Duration::MAX,
            Some(secs) => Duration::try_from_secs_f64(secs)
                .map_err(|_| StatementConfigError::request_timeout_conversion_failed(secs))?,
        };

        let mut p = self._inner.clone();

        p.set_request_timeout(Some(timeout));

        Ok(PreparedStatement { _inner: p })
    }

    fn without_request_timeout(&self) -> PreparedStatement {
        let mut p = self._inner.clone();
        p.set_request_timeout(None);
        PreparedStatement { _inner: p }
    }

    fn get_request_timeout(&self, py: Python<'_>) -> Py<PyAny> {
        match self._inner.get_request_timeout() {
            Some(t) if t == Duration::MAX => py.None(),
            Some(t) => PyFloat::new(py, t.as_secs_f64()).into(),
            None => UnsetType::get_instance(py).into(),
        }
    }

    fn with_page_size(&self, page_size: i32) -> PreparedStatement {
        let mut p = self._inner.clone();
        p.set_page_size(page_size);
        PreparedStatement { _inner: p }
    }

    fn get_page_size(&self) -> i32 {
        self._inner.get_page_size()
    }
}

#[pyclass(frozen)]
pub(crate) struct Statement {
    pub(crate) _inner: unprepared::Statement,
}

#[pymethods]
impl Statement {
    #[new]
    fn new(query_str: String) -> Statement {
        let s = unprepared::Statement::from(query_str);
        Statement { _inner: s }
    }

    #[getter]
    fn contents<'py>(&self, py: Python<'py>) -> Bound<'py, PyString> {
        PyString::new(py, &self._inner.contents)
    }

    fn with_execution_profile(&self, profile: ExecutionProfile) -> Statement {
        let mut s = self._inner.clone();
        s.set_execution_profile_handle(Some(profile._inner.into_handle()));
        Statement { _inner: s }
    }

    fn without_execution_profile(&self) -> Statement {
        let mut s = self._inner.clone();
        s.set_execution_profile_handle(None);
        Statement { _inner: s }
    }

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
        Statement { _inner: s }
    }

    fn without_consistency(&self) -> Statement {
        let mut s = self._inner.clone();
        s.unset_consistency();
        Statement { _inner: s }
    }

    fn get_consistency(&self) -> Option<Consistency> {
        self._inner.get_consistency().map(Consistency::to_python)
    }

    fn with_serial_consistency(&self, sc: Option<SerialConsistency>) -> Statement {
        let mut s = self._inner.clone();
        s.set_serial_consistency(sc.map(|sc| sc.to_rust()));
        Statement { _inner: s }
    }

    fn without_serial_consistency(&self) -> Statement {
        let mut s = self._inner.clone();
        s.unset_serial_consistency();
        Statement { _inner: s }
    }

    fn get_serial_consistency(&self) -> Option<SerialConsistency> {
        // TODO: implement returning Unset like in get_request_timeout
        self._inner
            .get_serial_consistency()
            .map(SerialConsistency::to_python)
    }

    fn with_request_timeout(
        &self,
        timeout: Option<f64>,
    ) -> Result<Statement, StatementConfigError> {
        if let Some(secs) = timeout
            && (!secs.is_finite() || secs <= 0.0)
        {
            return Err(StatementConfigError::InvalidRequestTimeout { value: secs });
        }

        let mut s = self._inner.clone();
        s.set_request_timeout(Some(timeout.map_or(Duration::MAX, Duration::from_secs_f64)));
        Ok(Statement { _inner: s })
    }

    fn without_request_timeout(&self) -> Statement {
        let mut s = self._inner.clone();
        s.set_request_timeout(None);
        Statement { _inner: s }
    }

    fn get_request_timeout(&self, py: Python<'_>) -> Py<PyAny> {
        match self._inner.get_request_timeout() {
            Some(t) if t == Duration::MAX => py.None(),
            Some(t) => PyFloat::new(py, t.as_secs_f64()).into(),
            None => UnsetType::get_instance(py).into(),
        }
    }

    fn with_page_size(&self, page_size: i32) -> Statement {
        let mut s = self._inner.clone();
        s.set_page_size(page_size);
        Statement { _inner: s }
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
