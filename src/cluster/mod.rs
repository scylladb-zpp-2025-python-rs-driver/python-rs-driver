use pyo3::prelude::*;

pub(crate) mod state;
use state::PyClusterState;

pub(crate) mod node;
use node::PyNode;

#[pymodule]
pub(crate) fn cluster(_py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<PyClusterState>()?;
    module.add_class::<PyNode>()?;
    Ok(())
}
