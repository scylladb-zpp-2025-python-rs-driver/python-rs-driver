use pyo3::{
    Bound, PyResult, Python,
    types::{PyAnyMethods, PyModule, PyModuleMethods},
};

/// Add submodule.
///
/// This function is required,
/// because by default for native libs python
/// adds module as an attribute and
/// doesn't add it's submodules in list
/// of all available modules.
///
/// To surpass this issue, we
/// manually update `sys.modules` attribute,
/// adding all submodules.
///
/// It's important to register submodules with
/// parent's full name in order to allow for
/// nested imports. Namely registering submodules
/// inside other submodules.
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
    module_constructor: impl FnOnce(Python<'_>, &Bound<'_, PyModule>) -> PyResult<()>,
) -> PyResult<()> {
    let full_name = format!("{}.{name}", parent_mod.name()?);
    let sub_module = PyModule::new(py, &full_name)?;
    module_constructor(py, &sub_module)?;
    parent_mod.add_submodule(&sub_module)?;
    py.import("sys")?
        .getattr("modules")?
        .set_item(&full_name, sub_module)?;
    Ok(())
}
