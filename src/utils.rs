use pyo3::{
    Bound, PyResult, Python,
    types::{PyAnyMethods, PyModule, PyModuleMethods},
};

pub(crate) struct PrependedIterator<I: ExactSizeIterator> {
    first: Option<I::Item>,
    rest: I,
}

impl<I: ExactSizeIterator> PrependedIterator<I> {
    pub(crate) fn new(first: I::Item, rest: I) -> Self {
        Self {
            first: Some(first),
            rest,
        }
    }
}

impl<I: ExactSizeIterator> Iterator for PrependedIterator<I> {
    type Item = I::Item;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(first) = self.first.take() {
            return Some(first);
        }
        self.rest.next()
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let len = self.len();
        (len, Some(len))
    }
}

impl<I: ExactSizeIterator> ExactSizeIterator for PrependedIterator<I> {
    fn len(&self) -> usize {
        self.rest.len() + usize::from(self.first.is_some())
    }
}

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
