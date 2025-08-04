use pyo3::{
    Bound, PyResult, Python,
    types::{PyAnyMethods, PyModule, PyModuleMethods},
};

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
    let sub_module = PyModule::new(py, name)?;
    module_constuctor(py, &sub_module)?;
    parent_mod.add_submodule(&sub_module)?;
    py.import("sys")?
        .getattr("modules")?
        .set_item(format!("{}.{name}", parent_mod.name()?), sub_module)?;
    Ok(())
}
