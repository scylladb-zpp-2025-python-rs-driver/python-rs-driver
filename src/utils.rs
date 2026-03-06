use pyo3::{
    Bound, Py, PyAny, PyRef, PyResult, Python, pyclass, pymethods,
    types::{PyAnyMethods, PyModule, PyModuleMethods},
};

#[pyclass]
pub(crate) struct GenericPyIterator {
    pub(crate) rust_iter: Box<dyn Iterator<Item = PyResult<Py<PyAny>>> + Send + Sync>,
}

#[pymethods]
impl GenericPyIterator {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(&mut self) -> Option<PyResult<Py<PyAny>>> {
        self.rust_iter.next()
    }
}

/// COPIED FROM SCYLLAPY
/// Add submodule.
///
/// This function is required,
/// because by default for native libs python
/// adds module as an attribute and
/// doesn't add it's submodules in list
/// of all available modules.
///
/// To surpass this issue, we
/// maually update `sys.modules` attribute,
/// adding all submodules.
///
/// # Errors
///
/// May result in an error, if
/// cannot construct modules, or add it,
/// or modify `sys.modules` attr.
pub(crate) fn add_submodule(
    py: Python<'_>,
    parent_mod: &Bound<'_, PyModule>,
    name: &'static str,
    module_constuctor: impl FnOnce(Python<'_>, &Bound<'_, PyModule>) -> PyResult<()>,
) -> PyResult<()> {
    let full_name = format!("{}.{name}", parent_mod.name()?);
    let sub_module = PyModule::new(py, &full_name)?;
    module_constuctor(py, &sub_module)?;
    parent_mod.add_submodule(&sub_module)?;
    py.import("sys")?
        .getattr("modules")?
        .set_item(&full_name, sub_module)?;
    Ok(())
}
