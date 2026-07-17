use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Condvar, Mutex};
use std::task::Wake;

use crate::RUNTIME;

use crate::coroutine::waker::AsyncioWaker;
use crate::coroutine::{Coroutine, PollResult};
use crate::utils::PrependedIterator;
use pyo3::exceptions::PyRuntimeError;
use pyo3::exceptions::PyStopIteration;
use pyo3::prelude::*;
use pyo3::sync::MutexExt;
use pyo3::types::{PyDict, PyTuple};
use pyo3::{BoundObject, Py, PyAny, PyResult};

use tokio::task::AbortHandle;

type BoxedFuture = Pin<Box<dyn Future<Output = PyResult<Py<PyAny>>> + Send>>;

// # PyResponseFuture — hybrid design
//
// ## Three states
//
// `PendingAsyncio { coroutine }`
//     The future is driven by the Python async protocol (`__next__`).
//     This is the default starting state.
//
// `PendingTokio { on_success, on_error, abort_handle, waker }`
//     The future has been spawned on the tokio runtime. `__next__` just
//     yields the asyncio future from the waker. The spawned task transitions
//     to `Ready` on completion.
//
// `Ready { result }`
//     Terminal state. Result stored permanently.
//
// ## Transitions
//
// - `PendingAsyncio` → `PendingTokio`: when callbacks are registered or `result()` is called.
//   The inner future is taken from the coroutine, spawned on tokio.
// - `PendingAsyncio` → `Ready`: when `poll` completes or `close()` is called.
// - `PendingTokio` → `Ready`: when the spawned task completes or `close()` aborts it.
// - `Ready` → (no transitions)

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

/// Internal state of a PyResponseFuture.
enum FutureState {
    /// Future is driven by the Python async protocol.
    PendingAsyncio { coroutine: Coroutine },
    /// Future has been spawned on the tokio runtime.
    PendingTokio {
        on_success: Vec<Callback>,
        on_error: Vec<Callback>,
        abort_handle: Option<AbortHandle>,
        waker: Arc<AsyncioWaker>,
    },
    /// Future has completed. Result is stored permanently.
    Ready { result: PyResult<Py<PyAny>> },
}

/// A Python awaitable wrapping a Rust future.
#[pyclass(name = "ResponseFuture", frozen)]
pub struct PyResponseFuture {
    state: Arc<Mutex<FutureState>>,
    /// Notified when state transitions to Ready.
    ready: Arc<Condvar>,
}

impl PyResponseFuture {
    /// Create a PyResponseFuture starting in PendingAsyncio (default).
    pub fn new<F>(future: F) -> Self
    where
        F: Future<Output = PyResult<Py<PyAny>>> + Send + 'static,
    {
        Self {
            state: Arc::new(Mutex::new(FutureState::PendingAsyncio {
                coroutine: Coroutine::new(None, future),
            })),
            ready: Arc::new(Condvar::new()),
        }
    }

    /// Create an already-resolved PyResponseFuture.
    pub fn ready(py: Python, result: PyResult<Py<PyAny>>) -> PyResult<Py<PyResponseFuture>> {
        Py::new(
            py,
            PyResponseFuture {
                state: Arc::new(Mutex::new(FutureState::Ready { result })),
                ready: Arc::new(Condvar::new()),
            },
        )
    }

    /// Spawn a future on tokio, returning the abort handle.
    /// On completion the spawned task transitions `state` to `Ready`,
    /// fires callbacks, wakes the asyncio waker, and notifies the condvar.
    fn spawn_future_on_tokio<F>(
        future: F,
        state: &Arc<Mutex<FutureState>>,
        ready: &Arc<Condvar>,
        waker: &Arc<AsyncioWaker>,
    ) -> AbortHandle
    where
        F: Future<Output = PyResult<Py<PyAny>>> + Send + 'static,
    {
        let state_clone = Arc::clone(state);
        let ready_clone = Arc::clone(ready);
        let waker_clone = Arc::clone(waker);

        let handle = RUNTIME.spawn(async move {
            let result = future.await;

            Python::attach(|py| {
                let callbacks = {
                    let mut state = state_clone.lock_py_attached(py).unwrap();
                    match &mut *state {
                        FutureState::PendingTokio {
                            on_success,
                            on_error,
                            ..
                        } => {
                            let taken_success = std::mem::take(on_success);
                            let taken_error = std::mem::take(on_error);
                            *state = FutureState::Ready {
                                result: clone_result(py, &result),
                            };
                            Some((taken_success, taken_error))
                        }
                        _ => None,
                    }
                };

                if let Some(cbs) = callbacks {
                    Callback::fire_all(py, cbs, &result);
                    waker_clone.wake();
                    ready_clone.notify_all();
                }
            });
        });

        handle.abort_handle()
    }

    /// Transition from PendingAsyncio to PendingTokio by spawning the given
    /// future on the tokio runtime.
    /// Must be called while holding the state lock.
    fn transition_to_tokio(
        future: BoxedFuture,
        waker: Arc<AsyncioWaker>,
        state: &Arc<Mutex<FutureState>>,
        ready: &Arc<Condvar>,
        state_guard: &mut std::sync::MutexGuard<'_, FutureState>,
    ) {
        let abort_handle = Self::spawn_future_on_tokio(future, state, ready, &waker);

        **state_guard = FutureState::PendingTokio {
            on_success: Vec::new(),
            on_error: Vec::new(),
            abort_handle: Some(abort_handle),
            waker,
        };
    }

    /// Poll the coroutine (__next__).
    fn poll_coroutine(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let mut state = self.state.lock_py_attached(py).unwrap();
        match &mut *state {
            FutureState::Ready { result } => raise_stop_iteration(py, result),

            FutureState::PendingTokio { waker, .. } => {
                // Future is running on tokio — just yield the asyncio future.
                let waker = Arc::clone(waker);
                drop(state);
                waker.yield_asyncio_future(py)
            }

            FutureState::PendingAsyncio { coroutine } => {
                // Drive the future via the coroutine.
                match coroutine.poll(py, None)? {
                    PollResult::Pending(value) => Ok(value),
                    PollResult::Ready(result) => {
                        *state = FutureState::Ready {
                            result: clone_result(py, &result),
                        };
                        drop(state);
                        self.ready.notify_all();
                        raise_stop_iteration(py, &result)
                    }
                }
            }
        }
    }

    /// Close the future. Transitions to Ready with an error.
    fn close_future(&self, py: Python<'_>) {
        let err_result: PyResult<Py<PyAny>> = Err(PyRuntimeError::new_err("future was closed"));

        let mut state = self.state.lock_py_attached(py).unwrap();
        match &mut *state {
            FutureState::Ready { .. } => (),

            FutureState::PendingTokio {
                abort_handle,
                waker,
                on_success,
                on_error,
                ..
            } => {
                if let Some(ah) = abort_handle {
                    ah.abort();
                }
                let waker = Arc::clone(waker);
                let taken_success = std::mem::take(on_success);
                let taken_error = std::mem::take(on_error);
                *state = FutureState::Ready {
                    result: clone_result(py, &err_result),
                };
                drop(state);

                waker.wake();
                self.ready.notify_all();
                Callback::fire_all(py, (taken_success, taken_error), &err_result);
            }

            FutureState::PendingAsyncio { coroutine } => {
                let waker = coroutine.close_and_get_waker();
                *state = FutureState::Ready {
                    result: clone_result(py, &err_result),
                };
                drop(state);

                if let Some(waker) = waker {
                    waker.wake();
                }
                self.ready.notify_all();
            }
        }
    }

    /// Release the GIL, wait on the condvar until state is Ready, then return the result.
    fn wait_for_ready(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        py.detach(|| {
            let state = self.state.lock().unwrap();
            let _state = self
                .ready
                .wait_while(state, |s| !matches!(s, FutureState::Ready { .. }))
                .unwrap();
        });

        let state = self.state.lock_py_attached(py).unwrap();
        match &*state {
            FutureState::Ready { result } => clone_result(py, result),
            _ => unreachable!("condvar woke but state is not Ready"),
        }
    }

    /// Block until the future is ready, returning the result.
    fn block_until_ready(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let mut state = self.state.lock_py_attached(py).unwrap();
        match &mut *state {
            FutureState::Ready { result } => clone_result(py, result),

            FutureState::PendingTokio { .. } => {
                drop(state);
                self.wait_for_ready(py)
            }

            FutureState::PendingAsyncio { coroutine } => {
                let (future, waker) = coroutine
                    .take_future_and_waker()
                    .expect("PendingAsyncio coroutine has no future");
                Self::transition_to_tokio(future, waker, &self.state, &self.ready, &mut state);
                drop(state);
                self.wait_for_ready(py)
            }
        }
    }
}

fn clone_result(py: Python<'_>, result: &PyResult<Py<PyAny>>) -> PyResult<Py<PyAny>> {
    match result {
        Ok(value) => Ok(value.clone_ref(py)),
        Err(err) => Err(err.clone_ref(py)),
    }
}

fn raise_stop_iteration(py: Python<'_>, result: &PyResult<Py<PyAny>>) -> PyResult<Py<PyAny>> {
    match result {
        Ok(value) => Err(PyStopIteration::new_err((value.clone_ref(py),))),
        Err(err) => Err(err.clone_ref(py)),
    }
}

#[pymethods]
impl PyResponseFuture {
    fn __await__(self_: Py<Self>) -> Py<Self> {
        self_
    }

    fn __iter__(self_: Py<Self>) -> Py<Self> {
        self_
    }

    fn __next__(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        self.poll_coroutine(py)
    }

    fn close(&self, py: Python<'_>) {
        self.close_future(py);
    }

    /// Get the result of this future.
    ///
    /// If the future is still pending, this blocks the calling thread until
    /// it completes (releasing the GIL while waiting).
    fn result(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        self.block_until_ready(py)
    }
}

#[pymodule]
pub(crate) fn future(_py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<PyResponseFuture>()?;
    Ok(())
}
