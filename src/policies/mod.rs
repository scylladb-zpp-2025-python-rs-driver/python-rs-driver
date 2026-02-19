use pyo3::prelude::*;

use crate::utils::add_submodule;

pub(crate) mod load_balancing;

#[pymodule]
pub(crate) fn policies(_py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    add_submodule(
        _py,
        module,
        "load_balancing",
        load_balancing::load_balancing,
    )?;
    Ok(())
}
