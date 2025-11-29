use std::sync::LazyLock;

use crate::deserialize::value;
use deserialize::results;
use pyo3::prelude::*;
use tokio::runtime::Runtime;

mod deserialize;
mod enums;
mod execution_profile;
mod serialize;
mod session;
mod session_builder;
mod statement;
mod types;
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
    add_submodule(py, module, "results", results::results)?;
    add_submodule(py, module, "statement", statement::statement)?;
    add_submodule(py, module, "enums", enums::enums)?;
    add_submodule(
        py,
        module,
        "execution_profile",
        execution_profile::execution_profile,
    )?;
    add_submodule(py, module, "types", types::types)?;
    add_submodule(py, module, "value", value::value)?;
    Ok(())
}
