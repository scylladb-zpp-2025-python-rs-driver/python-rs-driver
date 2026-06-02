use pyo3::prelude::*;

use crate::utils::add_submodule;

pub mod retry;

#[pymodule]
pub(crate) fn policies(py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    add_submodule(py, module, "retry_policy", retry::retry_policy)?;
    Ok(())
}
