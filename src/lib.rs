use std::sync::LazyLock;

use pyo3::prelude::*;
use tokio::runtime::Runtime;

mod session;
mod session_builder;
mod statement;
mod utils;

use crate::utils::add_submodule;

pub static RUNTIME: LazyLock<Runtime> = LazyLock::new(|| Runtime::new().unwrap());

/// A Python module implemented in Rust.
#[pymodule]
#[pyo3(name = "_rust")]
fn scylla(py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    add_submodule(
        py,
        module,
        "session_builder",
        session_builder::session_builder,
    )?;
    add_submodule(py, module, "session", session::session)?;
    add_submodule(py, module, "statement", statement::statement)?;
    Ok(())
}
