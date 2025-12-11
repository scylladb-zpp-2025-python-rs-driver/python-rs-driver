use pyo3::prelude::*;
use std::sync::OnceLock;

static UNSET_INSTANCE: OnceLock<Py<UnsetType>> = OnceLock::new();

#[pyclass]
pub(crate) struct UnsetType;

#[pymethods]
impl UnsetType {
    #[new]
    fn new(py: Python<'_>) -> PyResult<Py<UnsetType>> {
        Ok(UNSET_INSTANCE
            .get_or_init(|| Py::new(py, UnsetType).unwrap())
            .clone_ref(py))
    }

    fn __repr__(&self) -> &'static str {
        "Unset"
    }

    fn __str__(&self) -> &'static str {
        "Unset"
    }
}

impl UnsetType {
    pub(crate) fn get_instance(py: Python<'_>) -> PyResult<Py<UnsetType>> {
        Ok(UNSET_INSTANCE
            .get_or_init(|| Py::new(py, UnsetType).unwrap())
            .clone_ref(py))
    }
}

#[pymodule]
pub(crate) fn types(_py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<UnsetType>()?;
    Ok(())
}
