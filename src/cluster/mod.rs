use pyo3::prelude::*;

use crate::utils::add_submodule;

pub(crate) mod state;
use state::PyClusterState;

pub(crate) mod node;
use node::PyNode;
use node::PyNodeShard;

pub(crate) mod metadata;

#[pymodule]
pub(crate) fn cluster(_py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<PyClusterState>()?;
    module.add_class::<PyNodeShard>()?;
    module.add_class::<PyNode>()?;
    add_submodule(_py, module, "metadata", metadata::metadata)?;
    Ok(())
}
