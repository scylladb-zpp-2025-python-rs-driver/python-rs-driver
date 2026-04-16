use pyo3::prelude::*;
use pyo3::types::{PyFloat, PyString};
use scylla::statement::SerialConsistency;
use scylla::statement::prepared::PreparedStatement;
use scylla::statement::unprepared::Statement;
use std::time::Duration;

use crate::enums::{PyConsistency, PySerialConsistency};
use crate::errors::DriverStatementConfigError;
use crate::execution_profile::ExecutionProfile;
use crate::types::UnsetType;

#[pyclass(name = "PreparedStatement", frozen)]
pub(crate) struct PyPreparedStatement {
    pub(crate) _inner: PreparedStatement,
}

#[pymethods]
impl PyPreparedStatement {
    fn with_execution_profile(&self, profile: ExecutionProfile) -> PyPreparedStatement {
        let mut p = self._inner.clone();
        p.set_execution_profile_handle(Some(profile._inner.into_handle()));
        PyPreparedStatement { _inner: p }
    }

    fn without_execution_profile(&self) -> PyPreparedStatement {
        let mut p = self._inner.clone();
        p.set_execution_profile_handle(None);
        PyPreparedStatement { _inner: p }
    }

    fn get_execution_profile(&self) -> Option<ExecutionProfile> {
        self._inner
            .get_execution_profile_handle()
            .map(|h| ExecutionProfile {
                _inner: h.to_profile(),
            })
    }

    fn with_consistency(&self, c: PyConsistency) -> PyPreparedStatement {
        let mut p = self._inner.clone();
        p.set_consistency(c.into());
        PyPreparedStatement { _inner: p }
    }

    fn without_consistency(&self) -> PyPreparedStatement {
        let mut p = self._inner.clone();
        p.unset_consistency();
        PyPreparedStatement { _inner: p }
    }

    fn get_consistency(&self) -> Option<PyConsistency> {
        self._inner.get_consistency().map(PyConsistency::from)
    }

    fn with_serial_consistency(&self, sc: Option<PySerialConsistency>) -> PyPreparedStatement {
        let mut p = self._inner.clone();
        p.set_serial_consistency(sc.map(SerialConsistency::from));
        PyPreparedStatement { _inner: p }
    }

    fn without_serial_consistency(&self) -> PyPreparedStatement {
        let mut p = self._inner.clone();
        p.unset_serial_consistency();
        PyPreparedStatement { _inner: p }
    }

    fn get_serial_consistency(&self) -> Option<PySerialConsistency> {
        // TODO: implement returning Unset like in get_request_timeout
        self._inner
            .get_serial_consistency()
            .map(PySerialConsistency::from)
    }

    fn with_request_timeout(
        &self,
        timeout: Option<f64>,
    ) -> Result<PyPreparedStatement, DriverStatementConfigError> {
        if let Some(secs) = timeout
            && (!secs.is_finite() || secs <= 0.0)
        {
            return Err(DriverStatementConfigError::InvalidRequestTimeout { value: secs });
        }

        let timeout = match timeout {
            None => Duration::MAX,
            Some(secs) => Duration::try_from_secs_f64(secs)
                .map_err(|_| DriverStatementConfigError::request_timeout_conversion_failed(secs))?,
        };

        let mut p = self._inner.clone();

        p.set_request_timeout(Some(timeout));

        Ok(PyPreparedStatement { _inner: p })
    }

    fn without_request_timeout(&self) -> PyPreparedStatement {
        let mut p = self._inner.clone();
        p.set_request_timeout(None);
        PyPreparedStatement { _inner: p }
    }

    fn get_request_timeout(&self, py: Python<'_>) -> Py<PyAny> {
        match self._inner.get_request_timeout() {
            Some(t) if t == Duration::MAX => py.None(),
            Some(t) => PyFloat::new(py, t.as_secs_f64()).into(),
            None => UnsetType::get_instance(py).into(),
        }
    }

    fn with_page_size(&self, page_size: i32) -> PyPreparedStatement {
        let mut p = self._inner.clone();
        p.set_page_size(page_size);
        PyPreparedStatement { _inner: p }
    }

    fn get_page_size(&self) -> i32 {
        self._inner.get_page_size()
    }
}

#[pyclass(name = "Statement", frozen)]
pub(crate) struct PyStatement {
    pub(crate) _inner: Statement,
}

#[pymethods]
impl PyStatement {
    #[new]
    fn new(query_str: String) -> PyStatement {
        let s = Statement::from(query_str);
        PyStatement { _inner: s }
    }

    #[getter]
    fn contents<'py>(&self, py: Python<'py>) -> Bound<'py, PyString> {
        PyString::new(py, &self._inner.contents)
    }

    fn with_execution_profile(&self, profile: ExecutionProfile) -> PyStatement {
        let mut s = self._inner.clone();
        s.set_execution_profile_handle(Some(profile._inner.into_handle()));
        PyStatement { _inner: s }
    }

    fn without_execution_profile(&self) -> PyStatement {
        let mut s = self._inner.clone();
        s.set_execution_profile_handle(None);
        PyStatement { _inner: s }
    }

    fn get_execution_profile(&self) -> Option<ExecutionProfile> {
        self._inner
            .get_execution_profile_handle()
            .map(|h| ExecutionProfile {
                _inner: h.to_profile(),
            })
    }

    fn with_consistency(&self, c: PyConsistency) -> PyStatement {
        let mut s = self._inner.clone();
        s.set_consistency(c.into());
        PyStatement { _inner: s }
    }

    fn without_consistency(&self) -> PyStatement {
        let mut s = self._inner.clone();
        s.unset_consistency();
        PyStatement { _inner: s }
    }

    fn get_consistency(&self) -> Option<PyConsistency> {
        self._inner.get_consistency().map(PyConsistency::from)
    }

    fn with_serial_consistency(&self, sc: Option<PySerialConsistency>) -> PyStatement {
        let mut s = self._inner.clone();
        s.set_serial_consistency(sc.map(SerialConsistency::from));
        PyStatement { _inner: s }
    }

    fn without_serial_consistency(&self) -> PyStatement {
        let mut s = self._inner.clone();
        s.unset_serial_consistency();
        PyStatement { _inner: s }
    }

    fn get_serial_consistency(&self) -> Option<PySerialConsistency> {
        // TODO: implement returning Unset like in get_request_timeout
        self._inner
            .get_serial_consistency()
            .map(PySerialConsistency::from)
    }

    fn with_request_timeout(
        &self,
        timeout: Option<f64>,
    ) -> Result<PyStatement, DriverStatementConfigError> {
        if let Some(secs) = timeout
            && (!secs.is_finite() || secs <= 0.0)
        {
            return Err(DriverStatementConfigError::InvalidRequestTimeout { value: secs });
        }

        let mut s = self._inner.clone();
        s.set_request_timeout(Some(timeout.map_or(Duration::MAX, Duration::from_secs_f64)));
        Ok(PyStatement { _inner: s })
    }

    fn without_request_timeout(&self) -> PyStatement {
        let mut s = self._inner.clone();
        s.set_request_timeout(None);
        PyStatement { _inner: s }
    }

    fn get_request_timeout(&self, py: Python<'_>) -> Py<PyAny> {
        match self._inner.get_request_timeout() {
            Some(t) if t == Duration::MAX => py.None(),
            Some(t) => PyFloat::new(py, t.as_secs_f64()).into(),
            None => UnsetType::get_instance(py).into(),
        }
    }

    fn with_page_size(&self, page_size: i32) -> PyStatement {
        let mut s = self._inner.clone();
        s.set_page_size(page_size);
        PyStatement { _inner: s }
    }

    fn get_page_size(&self) -> i32 {
        self._inner.get_page_size()
    }
}

#[pymodule]
pub(crate) fn statement(_py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<PyPreparedStatement>()?;
    module.add_class::<PyStatement>()?;
    Ok(())
}
