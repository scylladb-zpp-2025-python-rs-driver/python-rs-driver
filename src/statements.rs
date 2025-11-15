use pyo3::prelude::{PyModule, PyModuleMethods};
use pyo3::{Bound, PyResult, Python, pyclass, pymethods, pymodule};
use scylla::statement::prepared::PreparedStatement;
use std::sync::Arc;

#[pyclass]
#[derive(Clone)]
pub struct PyPreparedStatement {
    pub _inner: Arc<PreparedStatement>,
}

#[pymethods]
impl PyPreparedStatement {
    pub fn get_columns_name(&self) -> Vec<String> {
        self._inner
            .get_variable_col_specs()
            .iter()
            .map(|col_spec| col_spec.name().to_string())
            .collect()
    }
}

#[pymodule]
pub(crate) fn statements(_py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<PyPreparedStatement>()?;

    Ok(())
}
