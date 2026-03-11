use crate::enums::{Consistency, SerialConsistency};
use crate::execution_profile::ExecutionProfile;
use crate::serialize::value_list::PyValueList;
use crate::session::ExecutableStatement;
use crate::types::UnsetType;
use pyo3::exceptions::PyValueError;
use pyo3::types::PyFloat;
use pyo3::{IntoPyObjectExt, prelude::*};
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
}

#[pymethods]
impl PyBatch {
    #[new]
    #[pyo3(signature = (batch_type=PyBatchType::Logged))]
    fn new(batch_type: PyBatchType) -> Self {
        Self {
            _inner: Batch::new(batch_type.into()),
            values: vec![],
            is_serial_consistency_set: false,
        }
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

    fn with_execution_profile(&self, profile: ExecutionProfile) -> PyBatch {
        let mut batch = self._inner.clone();
        batch.set_execution_profile_handle(Some(profile._inner.into_handle()));
        PyBatch {
            _inner: batch,
            values: self.values.clone(),
            is_serial_consistency_set: self.is_serial_consistency_set,
        }
    }

    fn without_execution_profile(&self) -> PyBatch {
        let mut batch = self._inner.clone();
        batch.set_execution_profile_handle(None);
        PyBatch {
            _inner: batch,
            values: self.values.clone(),
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

    fn with_consistency(&self, c: Consistency) -> PyBatch {
        let mut batch = self._inner.clone();
        batch.set_consistency(c.to_rust());
        PyBatch {
            _inner: batch,
            values: self.values.clone(),
            is_serial_consistency_set: self.is_serial_consistency_set,
        }
    }

    fn without_consistency(&self) -> PyBatch {
        let mut batch = self._inner.clone();
        batch.unset_consistency();
        PyBatch {
            _inner: batch,
            values: self.values.clone(),
            is_serial_consistency_set: self.is_serial_consistency_set,
        }
    }

    #[getter]
    fn get_consistency(&self) -> Option<Consistency> {
        self._inner.get_consistency().map(Consistency::to_python)
    }

    fn with_serial_consistency(&self, sc: Option<SerialConsistency>) -> PyBatch {
        let mut batch = self._inner.clone();
        batch.set_serial_consistency(sc.map(|sc| sc.to_rust()));
        PyBatch {
            _inner: batch,
            values: self.values.clone(),
            is_serial_consistency_set: true,
        }
    }

    fn without_serial_consistency(&self) -> PyBatch {
        let mut batch = self._inner.clone();
        batch.unset_serial_consistency();
        PyBatch {
            _inner: batch,
            values: self.values.clone(),
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

    fn with_request_timeout(&self, timeout: Option<f64>) -> PyResult<PyBatch> {
        if let Some(secs) = timeout
            && (!secs.is_finite() || secs <= 0.0)
        {
            return Err(PyValueError::new_err(
                "timeout must be a positive, finite number (in seconds)",
            ));
        }

        let timeout = match timeout {
            None => Duration::MAX,
            Some(secs) => Duration::try_from_secs_f64(secs)
                .map_err(|e| PyValueError::new_err(e.to_string()))?,
        };

        let mut batch = self._inner.clone();
        batch.set_request_timeout(Some(timeout));
        Ok(PyBatch {
            _inner: batch,
            values: self.values.clone(),
            is_serial_consistency_set: self.is_serial_consistency_set,
        })
    }

    fn without_request_timeout(&self) -> PyBatch {
        let mut batch = self._inner.clone();
        batch.set_request_timeout(None);
        PyBatch {
            _inner: batch,
            values: self.values.clone(),
            is_serial_consistency_set: self.is_serial_consistency_set,
        }
    }

    #[getter]
    fn get_request_timeout(&self, py: Python) -> PyResult<Py<PyAny>> {
        match self._inner.get_request_timeout() {
            Some(t) if t == Duration::MAX => Ok(py.None()),
            Some(t) => PyFloat::new(py, t.as_secs_f64()).into_py_any(py),
            None => UnsetType::get_instance(py).into_py_any(py),
        }
    }
}

#[pymodule]
pub(crate) fn batch(_py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<PyBatch>()?;
    module.add_class::<PyBatchType>()?;
    Ok(())
}
