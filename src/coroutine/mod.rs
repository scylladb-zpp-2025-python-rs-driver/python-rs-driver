// Portions of this file were copied from the PyO3 project (https://github.com/PyO3/pyo3),
// version 0.28.x (git commit: 8fcf8fc63), licensed under either of Apache-2.0 or MIT at your option.
//
// Copyright (c) 2023-present PyO3 Project and Contributors. https://github.com/PyO3
//
// Modifications Copyright 2025 ScyllaDB, licensed under Apache-2.0 OR MIT.
//
// Changes from the original pyo3 source:
//
// - `Coroutine` is no longer a `#[pyclass]`. It is used purely as internal Rust state,
//   not exposed to Python directly. `poll` returns a `PollResult` enum (`Pending` / `Ready`)
//   instead of a Python object, keeping the result in the Rust type system. This avoids
//   the overhead and error-prone nature of converting to Python objects before the caller
//   is ready to use them, and allows building higher-level abstractions on top using
//   full Rust type guarantees.
//
// - Imports updated from pyo3-internal paths (`alloc`, `core`, `pyo3_macros`, `crate::platform`)
//   to standard `std` and public `pyo3::` re-exports, since this code lives outside the pyo3
//   crate itself.
//
// - A `None` inner future in `poll` is now unreachable. The future is only `None` after
//   `close()` (which transitions `FutureState` to `Ready`) or `take_future_and_waker()`
//   (which transitions to `PendingTokio`). In neither case will `poll` be called on the
//   coroutine again, so the `None` branch is marked `unreachable!()`.

// - `take_future_and_waker` extracts the inner future so it can be spawned on Tokio,
//   transitioning the `FutureState` to `PendingTokio`. It returns an `Arc<AsyncioWaker>`
//   that is shared between the coroutine and the Tokio task. The waker is reset (its
//   internal asyncio future cleared) so that a fresh one can be created when needed.

// - `close_and_get_waker` drops the future and returns the waker so that the caller
//   (`PyResponseFuture::close`) can fire `waker.wake()` after writing `Ready`, ensuring any
//   Python coroutine suspended on this future gets rescheduled and sees the closed state.

// - Removed `unsafe impl Sync for Coroutine`. It is no longer needed because `Coroutine`
//   is not a `#[pyclass]` and lives behind a `Mutex`.

use std::future::Future;
use std::panic;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;

use crate::coroutine::cancel::ThrowCallback;
use crate::coroutine::waker::AsyncioWaker;

pub(crate) mod cancel;
pub(crate) mod waker;

type BoxedFuture = Pin<Box<dyn Future<Output = PyResult<Py<PyAny>>> + Send>>;

/// Result of polling a coroutine.
pub enum PollResult {
    /// The future is not ready. Yield this value to the Python event loop.
    /// Contains either an asyncio.Future object or py.None().
    Pending(Py<PyAny>),
    /// The future completed with this result.
    Ready(PyResult<Py<PyAny>>),
}

/// Rust-side coroutine wrapping a [`Future`].
pub(crate) struct Coroutine {
    throw_callback: Option<ThrowCallback>,
    future: Option<BoxedFuture>,
    waker: Option<Arc<AsyncioWaker>>,
}

impl Coroutine {
    /// Wrap a future into a coroutine.
    pub(crate) fn new<F>(throw_callback: Option<ThrowCallback>, future: F) -> Self
    where
        F: Future<Output = PyResult<Py<PyAny>>> + Send + 'static,
    {
        Self {
            throw_callback,
            future: Some(Box::pin(future)),
            waker: None,
        }
    }

    /// Takes the inner future and returns it together with the waker.
    /// Returns `None` if the future was already taken.
    pub(crate) fn take_future_and_waker(&mut self) -> Option<(BoxedFuture, Arc<AsyncioWaker>)> {
        let future = self.future.take()?;

        let waker = if let Some(existing) = &self.waker {
            Arc::clone(existing)
        } else {
            let new_waker = Arc::new(AsyncioWaker::new());
            self.waker = Some(Arc::clone(&new_waker));
            new_waker
        };
        Some((future, waker))
    }

    /// Poll the underlying future.
    pub(crate) fn poll(
        &mut self,
        py: Python<'_>,
        throw: Option<Py<PyAny>>,
    ) -> PyResult<PollResult> {
        // raise if the coroutine has already been run to completion
        let future_rs = match self.future {
            Some(ref mut fut) => fut,
            None => {
                // The future is `None` only after `close()` (which sets `FutureState::Ready`)
                // or `take_future_and_waker()` (which moves to `FutureState::PendingTokio`).
                // In both cases the `FutureState` is no longer `PendingAsyncio`, so `poll`
                // on the coroutine will never be called again.
                unreachable!()
            }
        };
        // reraise thrown exception
        match (throw, &self.throw_callback) {
            (Some(exc), Some(cb)) => cb.throw(exc),
            (Some(exc), None) => {
                self.close();
                return Ok(PollResult::Ready(Err(PyErr::from_value(
                    exc.into_bound(py),
                ))));
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
        let poll = || future_rs.as_mut().poll(&mut Context::from_waker(&waker));
        match std::panic::catch_unwind(panic::AssertUnwindSafe(poll)) {
            Ok(Poll::Ready(res)) => {
                self.close();
                return Ok(PollResult::Ready(res));
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
                return Ok(PollResult::Ready(Err(PyRuntimeError::new_err(msg))));
            }
            _ => {}
        }

        // unwrap() is safe as waker is always Some() when we reach here
        let value = self.waker.as_ref().unwrap().yield_asyncio_future(py)?;
        Ok(PollResult::Pending(value))
    }

    /// Close the coroutine, dropping the underlying future.
    /// Used when the future completed via `poll` — no waker needed since the
    /// state transition to `Ready` happens in the same call.
    pub(crate) fn close(&mut self) {
        drop(self.future.take());
    }

    /// Close the coroutine, dropping the underlying future, and return the waker.
    /// Used by `PyResponseFuture::close` so the caller can fire `waker.wake()` after
    /// writing `Ready`, waking any Python coroutine suspended on this future.
    pub(crate) fn close_and_get_waker(&mut self) -> Option<Arc<AsyncioWaker>> {
        drop(self.future.take());
        self.waker.take()
    }
}
