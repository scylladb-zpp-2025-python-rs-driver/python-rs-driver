use pyo3::prelude::*;

use crate::utils::add_submodule;

pub(crate) mod state;
use state::ClusterState;

pub(crate) mod node;
use node::Node;
use node::NodeShard;

pub(crate) mod metadata;

#[pymodule]
pub(crate) fn cluster(_py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<ClusterState>()?;
    module.add_class::<NodeShard>()?;
    module.add_class::<Node>()?;
    add_submodule(_py, module, "metadata", metadata::metadata)?;
    Ok(())
}
