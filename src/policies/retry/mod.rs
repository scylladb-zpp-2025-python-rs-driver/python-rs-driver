use pyo3::prelude::*;

pub mod types;

#[pymodule]
pub(crate) fn retry_policy(_py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<types::PyCqlResponseKind>()?;
    module.add_class::<types::PyOperationType>()?;
    module.add_class::<types::PyWriteType>()?;

    Ok(())
}
