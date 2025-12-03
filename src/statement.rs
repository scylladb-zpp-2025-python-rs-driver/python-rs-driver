use pyo3::prelude::*;
use scylla::statement::prepared;

#[pyclass(frozen)]
pub(crate) struct PreparedStatement {
    pub(crate) _inner: prepared::PreparedStatement,
}

#[pymodule]
pub(crate) fn statement(_py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<PreparedStatement>()?;
    Ok(())
}
