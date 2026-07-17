use crate::utils::PrependedIterator;
use pyo3::BoundObject;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyTuple};
use pyo3::{Py, PyAny, PyResult};
/// A registered callback with optional positional and keyword arguments.
struct Callback {
    callable: Py<PyAny>,
    args: Option<Py<PyTuple>>,
    kwargs: Option<Py<PyDict>>,
}

impl Callback {
    fn new(
        callable: Py<PyAny>,
        args: &Bound<'_, PyTuple>,
        kwargs: Option<&Bound<'_, PyDict>>,
    ) -> Self {
        Self {
            callable,
            args: if args.is_empty() {
                None
            } else {
                Some(args.clone().unbind())
            },
            kwargs: kwargs.map(|k| k.clone().unbind()),
        }
    }

    /// Invoke this callback, passing `value` as the first argument
    /// followed by any extra args/kwargs. Errors are logged and swallowed.
    fn invoke(&self, py: Python<'_>, value: &Py<PyAny>) {
        let args = if let Some(extra_args) = &self.args {
            let extra = extra_args.bind(py);
            let first = value.clone_ref(py).into_any();
            let rest = extra.iter().map(|item| item.unbind());
            let exact_size_wrapper = PrependedIterator::new(first, rest);
            PyTuple::new(py, exact_size_wrapper)
                .expect("failed to allocate PyTuple for callback args")
                .unbind()
        } else {
            PyTuple::new(py, [value.clone_ref(py)])
                .expect("failed to allocate PyTuple for callback args")
                .unbind()
        };

        let kwargs = self.kwargs.as_ref().map(|k| k.bind(py).clone());
        if let Err(err) = self.callable.call(py, args.bind(py), kwargs.as_ref()) {
            log::error!("ResponseFuture callback raised an exception: {}", err);
        }
    }

    /// Fire success or error callbacks based on the result.
    fn fire_all(
        py: Python<'_>,
        callbacks: (Vec<Callback>, Vec<Callback>),
        result: &PyResult<Py<PyAny>>,
    ) {
        let (on_success, on_error) = callbacks;
        match result {
            Ok(value) => {
                for cb in &on_success {
                    cb.invoke(py, value);
                }
            }
            Err(err) => {
                let err_obj = err.value(py);
                for cb in &on_error {
                    cb.invoke(py, err_obj.as_any().as_unbound());
                }
            }
        }
    }
}
