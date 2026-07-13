// Portions of this file were copied from the PyO3 project (https://github.com/PyO3/pyo3),
// version 0.28.x (git commit: 8fcf8fc63), licensed under either of Apache-2.0 or MIT at your option.

// Copyright (c) 2023-present PyO3 Project and Contributors. https://github.com/PyO3

// Modifications Copyright 2025 ScyllaDB, licensed under Apache-2.0 OR MIT.

use std::future::Future;
use std::panic;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

use pyo3::exceptions::{PyAttributeError, PyRuntimeError, PyStopIteration};
use pyo3::prelude::*;
use pyo3::types::{PyIterator, PyString};

use crate::coroutine::{cancel::ThrowCallback, waker::AsyncioWaker};

pub(crate) mod cancel;
mod waker;

const COROUTINE_REUSED_ERROR: &str = "cannot reuse already awaited coroutine";

/// Python coroutine wrapping a [`Future`].
#[pyclass]
pub struct Coroutine {
    name: Option<Py<PyString>>,
    qualname_prefix: Option<&'static str>,
    throw_callback: Option<ThrowCallback>,
    future: Option<Pin<Box<dyn Future<Output = PyResult<Py<PyAny>>> + Send>>>,
    waker: Option<Arc<AsyncioWaker>>,
}

// Safety: `Coroutine` is allowed to be `Sync` even though the future is not,
// because the future is polled with `&mut self` receiver
unsafe impl Sync for Coroutine {}

impl Coroutine {
    ///  Wrap a future into a Python coroutine.
    ///
    /// Coroutine `send` polls the wrapped future, ignoring the value passed
    /// (should always be `None` anyway).
    ///
    /// `Coroutine `throw` drop the wrapped future and reraise the exception passed
    pub(crate) fn new<'py, F>(
        name: Option<Bound<'py, PyString>>,
        qualname_prefix: Option<&'static str>,
        throw_callback: Option<ThrowCallback>,
        future: F,
    ) -> Self
    where
        F: Future<Output = Result<Py<PyAny>, PyErr>> + Send + 'static,
    {
        Self {
            name: name.map(Bound::unbind),
            qualname_prefix,
            throw_callback,
            future: Some(Box::pin(future)),
            waker: None,
        }
    }

    fn poll(&mut self, py: Python<'_>, throw: Option<Py<PyAny>>) -> PyResult<Py<PyAny>> {
        // raise if the coroutine has already been run to completion
        let future_rs = match self.future {
            Some(ref mut fut) => fut,
            None => return Err(PyRuntimeError::new_err(COROUTINE_REUSED_ERROR)),
        };
        // reraise thrown exception it
        match (throw, &self.throw_callback) {
            (Some(exc), Some(cb)) => cb.throw(exc),
            (Some(exc), None) => {
                self.close();
                return Err(PyErr::from_value(exc.into_bound(py)));
            }
            (None, _) => {}
        }
        // create a new waker, or try to reset it in place
        if let Some(waker) = self.waker.as_mut().and_then(Arc::get_mut) {
            waker.reset();
        } else {
            self.waker = Some(Arc::new(AsyncioWaker::new()));
        }
        let waker = Waker::from(self.waker.clone().unwrap());
        // poll the Rust future and forward its results if ready
        // polling is UnwindSafe because the future is dropped in case of panic
        let poll = || future_rs.as_mut().poll(&mut Context::from_waker(&waker));
        match std::panic::catch_unwind(panic::AssertUnwindSafe(poll)) {
            Ok(Poll::Ready(res)) => {
                self.close();
                return Err(PyStopIteration::new_err((res?,)));
            }
            Err(err) => {
                self.close();
                let msg = if let Some(s) = err.downcast_ref::<&str>() {
                    s.to_string()
                } else if let Some(s) = err.downcast_ref::<String>() {
                    s.clone()
                } else {
                    "Rust future panicked".to_string()
                };
                return Err(PyRuntimeError::new_err(msg));
            }
            _ => {}
        }
        // otherwise, initialize the waker `asyncio.Future`
        if let Some(future) = self.waker.as_ref().unwrap().initialize_future(py)? {
            // `asyncio.Future` must be awaited; fortunately, it implements `__iter__ = __await__`
            // and will yield itself if its result has not been set in polling above
            if let Some(future) = PyIterator::from_object(future).unwrap().next() {
                // future has not been leaked into Python for now, and Rust code can only call
                // `set_result(None)` in `Wake` implementation, so it's safe to unwrap
                return Ok(future.unwrap().into());
            }
        }
        // if waker has been waken during future polling, this is roughly equivalent to
        // `await asyncio.sleep(0)`, so just yield `None`.
        Ok(py.None())
    }
}

#[pymethods]
impl Coroutine {
    #[getter]
    fn __name__(&self, py: Python<'_>) -> PyResult<Py<PyString>> {
        match &self.name {
            Some(name) => Ok(name.clone_ref(py)),
            None => Err(PyAttributeError::new_err("__name__")),
        }
    }

    #[getter]
    fn __qualname__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyString>> {
        match (&self.name, &self.qualname_prefix) {
            (Some(name), Some(prefix)) => Ok(PyString::new(
                py,
                &format!("{}.{}", prefix, name.bind(py).to_str()?),
            )),
            (Some(name), None) => Ok(name.bind(py).clone()),
            (None, _) => Err(PyAttributeError::new_err("__qualname__")),
        }
    }

    fn send(&mut self, py: Python<'_>, _value: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        self.poll(py, None)
    }

    fn throw(&mut self, py: Python<'_>, exc: Py<PyAny>) -> PyResult<Py<PyAny>> {
        self.poll(py, Some(exc))
    }

    fn close(&mut self) {
        // the Rust future is dropped, and the field set to `None`
        // to indicate the coroutine has been run to completion
        drop(self.future.take());
    }

    fn __await__(self_: Py<Self>) -> Py<Self> {
        self_
    }

    fn __next__(&mut self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        self.poll(py, None)
    }
}
