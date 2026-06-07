use crate::enums::{PyConsistency, PySerialConsistency};
use crate::errors::DriverBatchError;
use crate::execution_profile::PyExecutionProfile;
use crate::policies::retry::policies::py_any_to_arc_retry_policy;
use crate::serialize::value_list::PyValueList;
use crate::session::ExecutableStatement;
use crate::types::UnsetType;
use pyo3::types::PyFloat;
use pyo3::{IntoPyObjectExt, prelude::*};
use scylla::statement::SerialConsistency;
use scylla::statement::batch::{Batch, BatchType};
use std::time::Duration;

#[pyclass(name = "BatchType", from_py_object, eq, eq_int, frozen)]
#[derive(Clone, Copy, PartialEq)]
pub(crate) enum PyBatchType {
    Logged,
    Unlogged,
    Counter,
}

impl From<PyBatchType> for BatchType {
    fn from(value: PyBatchType) -> Self {
        match value {
            PyBatchType::Logged => Self::Logged,
            PyBatchType::Unlogged => Self::Unlogged,
            PyBatchType::Counter => Self::Counter,
        }
    }
}

impl From<BatchType> for PyBatchType {
    fn from(value: BatchType) -> Self {
        match value {
            BatchType::Logged => Self::Logged,
            BatchType::Unlogged => Self::Unlogged,
            BatchType::Counter => Self::Counter,
        }
    }
}

#[pyclass(name = "Batch", from_py_object)]
#[derive(Clone)]
pub(crate) struct PyBatch {
    pub(crate) _inner: Batch,
    pub(crate) values: Vec<PyValueList>,
    // Because `get_serial_consistency` in the Rust driver returns `Option<SerialConsistency>`,
    // it cannot represent the `Unset` state. Therefore, the Python-rs driver must distinguish
    // between `Unset` and `None` in a different way. To preserve this distinction, an additional
    // flag `is_serial_consistency_set` is required.
    is_serial_consistency_set: bool,
    pub(crate) _retry_policy: Option<Py<PyAny>>,
    pub(crate) _execution_profile: Option<Py<PyExecutionProfile>>,
}

impl PyBatch {
    pub(crate) fn new(
        _inner: Batch,
        values: Vec<PyValueList>,
        is_serial_consistency_set: bool,
        _retry_policy: Option<Py<PyAny>>,
        _execution_profile: Option<Py<PyExecutionProfile>>,
    ) -> Self {
        Self {
            _inner,
            values,
            is_serial_consistency_set,
            _retry_policy,
            _execution_profile,
        }
    }
}

#[pymethods]
impl PyBatch {
    #[new]
    #[pyo3(signature = (batch_type=PyBatchType::Logged))]
    fn py_new(batch_type: PyBatchType) -> Self {
        Self::new(Batch::new(batch_type.into()), vec![], false, None, None)
    }

    #[pyo3(signature = (statement, values=None))]
    fn add(&mut self, statement: ExecutableStatement, values: Option<PyValueList>) {
        self._inner.append_statement(statement);
        self.values.push(values.unwrap_or(PyValueList::Empty));
    }

    fn add_all(&mut self, items: Vec<(ExecutableStatement, Option<PyValueList>)>) {
        self.values.reserve_exact(items.len());
        for (statement, values) in items {
            self.add(statement, values);
        }
    }

    #[getter]
    fn get_type(&self) -> PyBatchType {
        self._inner.get_type().into()
    }

    fn with_execution_profile(&self, profile: Py<PyExecutionProfile>) -> Self {
        let mut batch = self._inner.clone();
        let inner = profile.get()._inner.clone();
        batch.set_execution_profile_handle(Some(inner.into_handle()));

        Self::new(
            batch,
            self.values.clone(),
            self.is_serial_consistency_set,
            self._retry_policy.clone(),
            Some(profile),
        )
    }

    fn without_execution_profile(&self) -> Self {
        let mut batch = self._inner.clone();
        batch.set_execution_profile_handle(None);

        Self::new(
            batch,
            self.values.clone(),
            self.is_serial_consistency_set,
            self._retry_policy.clone(),
            None,
        )
    }

    #[getter]
    fn get_execution_profile(&self) -> Option<Py<PyExecutionProfile>> {
        self._execution_profile.clone()
    }

    fn with_consistency(&self, c: PyConsistency) -> Self {
        let mut batch = self._inner.clone();
        batch.set_consistency(c.into());

        Self::new(
            batch,
            self.values.clone(),
            self.is_serial_consistency_set,
            self._retry_policy.clone(),
            self._execution_profile.clone(),
        )
    }

    fn without_consistency(&self) -> Self {
        let mut batch = self._inner.clone();
        batch.unset_consistency();

        Self::new(
            batch,
            self.values.clone(),
            self.is_serial_consistency_set,
            self._retry_policy.clone(),
            self._execution_profile.clone(),
        )
    }

    #[getter]
    fn get_consistency(&self) -> Option<PyConsistency> {
        self._inner.get_consistency().map(PyConsistency::from)
    }

    fn with_serial_consistency(&self, sc: Option<PySerialConsistency>) -> Self {
        let mut batch = self._inner.clone();
        batch.set_serial_consistency(sc.map(SerialConsistency::from));

        Self::new(
            batch,
            self.values.clone(),
            true,
            self._retry_policy.clone(),
            self._execution_profile.clone(),
        )
    }

    fn without_serial_consistency(&self) -> Self {
        let mut batch = self._inner.clone();
        batch.unset_serial_consistency();

        Self::new(
            batch,
            self.values.clone(),
            false,
            self._retry_policy.clone(),
            self._execution_profile.clone(),
        )
    }

    #[getter]
    fn get_serial_consistency(&self, py: Python) -> Result<Py<PyAny>, DriverBatchError> {
        if !self.is_serial_consistency_set {
            return UnsetType::get_instance(py)
                .into_py_any(py)
                .map_err(DriverBatchError::python_conversion_failed);
        }
        match self._inner.get_serial_consistency() {
            Some(sc) => PySerialConsistency::from(sc)
                .into_py_any(py)
                .map_err(DriverBatchError::python_conversion_failed),
            None => Ok(py.None()),
        }
    }

    fn with_request_timeout(&self, timeout: Option<f64>) -> Result<Self, DriverBatchError> {
        let timeout = match timeout {
            None => Duration::MAX,
            Some(secs) => Duration::try_from_secs_f64(secs)
                .map_err(|_| DriverBatchError::invalid_request_timeout(secs))?,
        };

        let mut batch = self._inner.clone();
        batch.set_request_timeout(Some(timeout));

        Ok(Self::new(
            batch,
            self.values.clone(),
            self.is_serial_consistency_set,
            self._retry_policy.clone(),
            self._execution_profile.clone(),
        ))
    }

    fn without_request_timeout(&self) -> Self {
        let mut batch = self._inner.clone();
        batch.set_request_timeout(None);

        Self::new(
            batch,
            self.values.clone(),
            self.is_serial_consistency_set,
            self._retry_policy.clone(),
            self._execution_profile.clone(),
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

    fn with_retry_policy(&self, retry_policy: Py<PyAny>, py: Python<'_>) -> Self {
        let mut batch = self._inner.clone();
        let arc = py_any_to_arc_retry_policy(&retry_policy, py);
        batch.set_retry_policy(Some(arc));

        Self::new(
            batch,
            self.values.clone(),
            self.is_serial_consistency_set,
            Some(retry_policy),
            self._execution_profile.clone(),
        )
    }

    fn without_retry_policy(&self) -> Self {
        let mut batch = self._inner.clone();
        batch.set_retry_policy(None);

        Self::new(
            batch,
            self.values.clone(),
            self.is_serial_consistency_set,
            None,
            self._execution_profile.clone(),
        )
    }

    #[getter]
    fn get_retry_policy(&self, py: Python<'_>) -> Option<Py<PyAny>> {
        self._retry_policy.as_ref().map(|rp| rp.clone_ref(py))
    }

    fn set_is_idempotent(&self, is_idempotent: bool) -> Self {
        let mut batch = self._inner.clone();
        batch.set_is_idempotent(is_idempotent);

        Self::new(
            batch,
            self.values.clone(),
            self.is_serial_consistency_set,
            self._retry_policy.clone(),
            self._execution_profile.clone(),
        )
    }

    #[getter]
    fn get_is_idempotent(&self) -> bool {
        self._inner.get_is_idempotent()
    }
}

#[pymodule]
pub(crate) fn batch(_py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<PyBatch>()?;
    module.add_class::<PyBatchType>()?;
    Ok(())
}
