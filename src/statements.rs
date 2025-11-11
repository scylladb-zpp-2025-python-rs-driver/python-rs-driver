use pyo3::prelude::{PyModule, PyModuleMethods};
use pyo3::{Bound, PyResult, Python, pyclass, pymodule};
use scylla::statement::prepared::PreparedStatement;
use std::sync::Arc;

#[pyclass]
#[derive(Clone)]
pub struct PyPreparedStatement {
    pub _inner: Arc<PreparedStatement>,
}

#[pymodule]
pub(crate) fn statements(_py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<PyPreparedStatement>()?;

    Ok(())
}
