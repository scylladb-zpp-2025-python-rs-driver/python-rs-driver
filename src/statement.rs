use pyo3::IntoPyObjectExt;
use pyo3::prelude::*;
use pyo3::types::{PyFloat, PyString};
use scylla::statement::SerialConsistency;
use scylla::statement::prepared::PreparedStatement;
use scylla::statement::unprepared::Statement;
use std::time::Duration;

use crate::enums::{PyConsistency, PySerialConsistency};
use crate::errors::DriverStatementConfigError;
use crate::execution_profile::ExecutionProfile;
use crate::load_balancing::PyLoadBalancingPolicy;
use crate::types::UnsetType;

#[pyclass(name = "PreparedStatement", frozen)]
pub(crate) struct PyPreparedStatement {
    pub(crate) _inner: PreparedStatement,
    // Because `get_serial_consistency` in the Rust driver returns `Option<SerialConsistency>`,
    // it cannot represent the `Unset` state. Therefore, the Python-rs driver must distinguish
    // between `Unset` and `None` in a different way. To preserve this distinction, an additional
    // flag `is_serial_consistency_set` is required.
    is_serial_consistency_set: bool,
    pub(crate) _execution_profile: Option<Py<ExecutionProfile>>,
    pub(crate) _load_balancing_policy: Option<Py<PyAny>>,
}

impl PyPreparedStatement {
    pub(crate) fn new(
        _inner: PreparedStatement,
        is_serial_consistency_set: bool,
        _execution_profile: Option<Py<ExecutionProfile>>,
        _load_balancing_policy: Option<Py<PyAny>>,
    ) -> Self {
        Self {
            _inner,
            is_serial_consistency_set,
            _execution_profile,
            _load_balancing_policy,
        }
    }
}

#[pymethods]
impl PyPreparedStatement {
    fn with_execution_profile(&self, profile: Py<ExecutionProfile>) -> Self {
        let mut p = self._inner.clone();
        p.set_execution_profile_handle(Some(profile.get()._inner.clone().into_handle()));
        Self::new(
            p,
            self.is_serial_consistency_set,
            Some(profile),
            self._load_balancing_policy.clone(),
        )
    }

    fn without_execution_profile(&self) -> Self {
        let mut p = self._inner.clone();
        p.set_execution_profile_handle(None);
        Self::new(
            p,
            self.is_serial_consistency_set,
            None,
            self._load_balancing_policy.clone(),
        )
    }

    #[getter]
    fn get_execution_profile(&self) -> Option<Py<ExecutionProfile>> {
        self._execution_profile.clone()
    }

    fn with_load_balancing_policy(
        &self,
        py: Python<'_>,
        py_policy: Py<PyAny>,
    ) -> Result<Self, DriverStatementConfigError> {
        let policy = py_policy.bind(py).extract::<PyLoadBalancingPolicy>()?;
        let mut p = self._inner.clone();
        p.set_load_balancing_policy(Some(policy.into_inner()));
        Ok(Self::new(
            p,
            self.is_serial_consistency_set,
            self._execution_profile.clone(),
            Some(py_policy),
        ))
    }

    fn without_load_balancing_policy(&self) -> Self {
        let mut p = self._inner.clone();
        p.set_load_balancing_policy(None);
        Self::new(
            p,
            self.is_serial_consistency_set,
            self._execution_profile.clone(),
            None,
        )
    }

    #[getter]
    fn get_load_balancing_policy(&self) -> Option<Py<PyAny>> {
        self._load_balancing_policy.clone()
    }

    fn with_consistency(&self, c: PyConsistency) -> Self {
        let mut p = self._inner.clone();
        p.set_consistency(c.into());
        Self::new(
            p,
            self.is_serial_consistency_set,
            self._execution_profile.clone(),
            self._load_balancing_policy.clone(),
        )
    }

    fn without_consistency(&self) -> Self {
        let mut p = self._inner.clone();
        p.unset_consistency();
        Self::new(
            p,
            self.is_serial_consistency_set,
            self._execution_profile.clone(),
            self._load_balancing_policy.clone(),
        )
    }

    #[getter]
    fn get_consistency(&self) -> Option<PyConsistency> {
        self._inner.get_consistency().map(PyConsistency::from)
    }

    fn with_serial_consistency(&self, sc: Option<PySerialConsistency>) -> Self {
        let mut p = self._inner.clone();
        p.set_serial_consistency(sc.map(SerialConsistency::from));
        Self::new(
            p,
            true,
            self._execution_profile.clone(),
            self._load_balancing_policy.clone(),
        )
    }

    fn without_serial_consistency(&self) -> Self {
        let mut p = self._inner.clone();
        p.unset_serial_consistency();
        Self::new(
            p,
            false,
            self._execution_profile.clone(),
            self._load_balancing_policy.clone(),
        )
    }

    #[getter]
    fn get_serial_consistency(&self, py: Python) -> Result<Py<PyAny>, DriverStatementConfigError> {
        if !self.is_serial_consistency_set {
            return UnsetType::get_instance(py)
                .into_py_any(py)
                .map_err(DriverStatementConfigError::python_conversion_failed);
        }
        match self._inner.get_serial_consistency() {
            Some(sc) => PySerialConsistency::from(sc)
                .into_py_any(py)
                .map_err(DriverStatementConfigError::python_conversion_failed),
            None => Ok(py.None()),
        }
    }

    fn with_request_timeout(
        &self,
        timeout: Option<f64>,
    ) -> Result<Self, DriverStatementConfigError> {
        let timeout = match timeout {
            None => Duration::MAX,
            Some(secs) => Duration::try_from_secs_f64(secs)
                .map_err(|_| DriverStatementConfigError::invalid_request_timeout(secs))?,
        };

        let mut p = self._inner.clone();

        p.set_request_timeout(Some(timeout));

        Ok(Self::new(
            p,
            self.is_serial_consistency_set,
            self._execution_profile.clone(),
            self._load_balancing_policy.clone(),
        ))
    }

    fn without_request_timeout(&self) -> Self {
        let mut p = self._inner.clone();
        p.set_request_timeout(None);
        Self::new(
            p,
            self.is_serial_consistency_set,
            self._execution_profile.clone(),
            self._load_balancing_policy.clone(),
        )
    }

    #[getter]
    fn get_request_timeout(&self, py: Python<'_>) -> Py<PyAny> {
        match self._inner.get_request_timeout() {
            Some(t) if t == Duration::MAX => py.None(),
            Some(t) => PyFloat::new(py, t.as_secs_f64()).into(),
            None => UnsetType::get_instance(py).into(),
        }
    }

    fn with_page_size(&self, page_size: i32) -> Self {
        let mut p = self._inner.clone();
        p.set_page_size(page_size);
        Self::new(
            p,
            self.is_serial_consistency_set,
            self._execution_profile.clone(),
            self._load_balancing_policy.clone(),
        )
    }

    #[getter]
    fn get_page_size(&self) -> i32 {
        self._inner.get_page_size()
    }
}

#[pyclass(name = "Statement", frozen)]
pub(crate) struct PyStatement {
    pub(crate) _inner: Statement,
    // Because `get_serial_consistency` in the Rust driver returns `Option<SerialConsistency>`,
    // it cannot represent the `Unset` state. Therefore, the Python-rs driver must distinguish
    // between `Unset` and `None` in a different way. To preserve this distinction, an additional
    // flag `is_serial_consistency_set` is required.
    is_serial_consistency_set: bool,
    pub(crate) _execution_profile: Option<Py<ExecutionProfile>>,
    pub(crate) _load_balancing_policy: Option<Py<PyAny>>,
}

impl PyStatement {
    pub(crate) fn new(
        _inner: Statement,
        is_serial_consistency_set: bool,
        _execution_profile: Option<Py<ExecutionProfile>>,
        _load_balancing_policy: Option<Py<PyAny>>,
    ) -> Self {
        Self {
            _inner,
            is_serial_consistency_set,
            _execution_profile,
            _load_balancing_policy,
        }
    }
}

#[pymethods]
impl PyStatement {
    #[new]
    fn py_new(query_str: String) -> Self {
        let s = Statement::from(query_str);
        Self::new(s, false, None, None)
    }

    #[getter]
    fn contents<'py>(&self, py: Python<'py>) -> Bound<'py, PyString> {
        PyString::new(py, &self._inner.contents)
    }

    fn with_execution_profile(&self, profile: Py<ExecutionProfile>) -> Self {
        let mut s = self._inner.clone();
        s.set_execution_profile_handle(Some(profile.get()._inner.clone().into_handle()));
        Self::new(
            s,
            self.is_serial_consistency_set,
            Some(profile),
            self._load_balancing_policy.clone(),
        )
    }

    fn without_execution_profile(&self) -> Self {
        let mut s = self._inner.clone();
        s.set_execution_profile_handle(None);
        Self::new(
            s,
            self.is_serial_consistency_set,
            None,
            self._load_balancing_policy.clone(),
        )
    }

    #[getter]
    fn get_execution_profile(&self) -> Option<Py<ExecutionProfile>> {
        self._execution_profile.clone()
    }

    fn with_load_balancing_policy(
        &self,
        py: Python<'_>,
        py_policy: Py<PyAny>,
    ) -> Result<Self, DriverStatementConfigError> {
        let policy = py_policy.bind(py).extract::<PyLoadBalancingPolicy>()?;
        let mut s = self._inner.clone();
        s.set_load_balancing_policy(Some(policy.into_inner()));
        Ok(Self::new(
            s,
            self.is_serial_consistency_set,
            self._execution_profile.clone(),
            Some(py_policy),
        ))
    }

    fn without_load_balancing_policy(&self) -> Self {
        let mut s = self._inner.clone();
        s.set_load_balancing_policy(None);
        Self::new(
            s,
            self.is_serial_consistency_set,
            self._execution_profile.clone(),
            None,
        )
    }

    #[getter]
    fn get_load_balancing_policy(&self) -> Option<Py<PyAny>> {
        self._load_balancing_policy.clone()
    }

    fn with_consistency(&self, c: PyConsistency) -> Self {
        let mut s = self._inner.clone();
        s.set_consistency(c.into());
        Self::new(
            s,
            self.is_serial_consistency_set,
            self._execution_profile.clone(),
            self._load_balancing_policy.clone(),
        )
    }

    fn without_consistency(&self) -> Self {
        let mut s = self._inner.clone();
        s.unset_consistency();
        Self::new(
            s,
            self.is_serial_consistency_set,
            self._execution_profile.clone(),
            self._load_balancing_policy.clone(),
        )
    }

    #[getter]
    fn get_consistency(&self) -> Option<PyConsistency> {
        self._inner.get_consistency().map(PyConsistency::from)
    }

    fn with_serial_consistency(&self, sc: Option<PySerialConsistency>) -> Self {
        let mut s = self._inner.clone();
        s.set_serial_consistency(sc.map(SerialConsistency::from));
        Self::new(
            s,
            true,
            self._execution_profile.clone(),
            self._load_balancing_policy.clone(),
        )
    }

    fn without_serial_consistency(&self) -> Self {
        let mut s = self._inner.clone();
        s.unset_serial_consistency();
        Self::new(
            s,
            false,
            self._execution_profile.clone(),
            self._load_balancing_policy.clone(),
        )
    }

    #[getter]
    fn get_serial_consistency(&self, py: Python) -> Result<Py<PyAny>, DriverStatementConfigError> {
        if !self.is_serial_consistency_set {
            return UnsetType::get_instance(py)
                .into_py_any(py)
                .map_err(DriverStatementConfigError::python_conversion_failed);
        }
        match self._inner.get_serial_consistency() {
            Some(sc) => PySerialConsistency::from(sc)
                .into_py_any(py)
                .map_err(DriverStatementConfigError::python_conversion_failed),
            None => Ok(py.None()),
        }
    }

    fn with_request_timeout(
        &self,
        timeout: Option<f64>,
    ) -> Result<Self, DriverStatementConfigError> {
        let timeout = match timeout {
            None => Duration::MAX,
            Some(secs) => Duration::try_from_secs_f64(secs)
                .map_err(|_| DriverStatementConfigError::invalid_request_timeout(secs))?,
        };

        let mut s = self._inner.clone();
        s.set_request_timeout(Some(timeout));
        Ok(Self::new(
            s,
            self.is_serial_consistency_set,
            self._execution_profile.clone(),
            self._load_balancing_policy.clone(),
        ))
    }

    fn without_request_timeout(&self) -> Self {
        let mut s = self._inner.clone();
        s.set_request_timeout(None);
        Self::new(
            s,
            self.is_serial_consistency_set,
            self._execution_profile.clone(),
            self._load_balancing_policy.clone(),
        )
    }

    #[getter]
    fn get_request_timeout(&self, py: Python<'_>) -> Py<PyAny> {
        match self._inner.get_request_timeout() {
            Some(t) if t == Duration::MAX => py.None(),
            Some(t) => PyFloat::new(py, t.as_secs_f64()).into(),
            None => UnsetType::get_instance(py).into(),
        }
    }

    fn with_page_size(&self, page_size: i32) -> Self {
        let mut s = self._inner.clone();
        s.set_page_size(page_size);
        Self::new(
            s,
            self.is_serial_consistency_set,
            self._execution_profile.clone(),
            self._load_balancing_policy.clone(),
        )
    }

    #[getter]
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
