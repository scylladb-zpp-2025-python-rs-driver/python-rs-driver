// Portions of this file were copied from the PyO3 project (https://github.com/PyO3/pyo3),
// version 0.28.x (git commit: 8fcf8fc63), licensed under either of Apache-2.0 or MIT at your option.
//
// Copyright (c) 2023-present PyO3 Project and Contributors. https://github.com/PyO3
//
// Modifications Copyright 2025 ScyllaDB, licensed under Apache-2.0 OR MIT.

use std::sync::{Arc, Mutex};
use std::task::Waker;

use pyo3::{Py, PyAny};

#[derive(Debug, Default)]
struct Inner {
    exception: Option<Py<PyAny>>,
    waker: Option<Waker>,
}

/// Callback used by the coroutine to deliver a thrown exception to the [`CancelHandle`].
pub struct ThrowCallback(Arc<Mutex<Inner>>);

impl ThrowCallback {
    pub(crate) fn throw(&self, exc: Py<PyAny>) {
        let mut inner = self.0.lock().unwrap();
        inner.exception = Some(exc);
        if let Some(waker) = inner.waker.take() {
            waker.wake();
        }
    }
}
