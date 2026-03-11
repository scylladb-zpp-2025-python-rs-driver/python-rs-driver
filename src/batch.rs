use crate::serialize::value_list::PyValueList;
use crate::session::ExecutableStatement;
use pyo3::prelude::*;
use scylla::statement::batch::{Batch, BatchType};

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
}

#[pymethods]
impl PyBatch {
    #[new]
    #[pyo3(signature = (batch_type=PyBatchType::Logged))]
    fn new(batch_type: PyBatchType) -> Self {
        Self {
            _inner: Batch::new(batch_type.into()),
            values: vec![],
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
}

#[pymodule]
pub(crate) fn batch(_py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<PyBatch>()?;
    module.add_class::<PyBatchType>()?;
    Ok(())
}
