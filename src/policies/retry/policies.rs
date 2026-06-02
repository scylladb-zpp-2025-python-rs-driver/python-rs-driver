use crate::policies::retry::decision::PyRetryDecision;
use crate::policies::retry::request::PyRequestInfo;
use pyo3::intern;
use pyo3::prelude::*;
use pyo3::sync::MutexExt;
use scylla::policies::retry::{
    DefaultRetryPolicy, DefaultRetrySession, DowngradingConsistencyRetryPolicy,
    FallthroughRetryPolicy, RequestInfo, RetryDecision, RetryPolicy, RetrySession,
};
use scylla::policies::retry::{DowngradingConsistencyRetrySession, FallthroughRetrySession};
use std::sync::Arc;
use std::sync::Mutex;
use tracing::error;

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub(crate) struct PyCustomRetrySession {
    pub(crate) _inner: Py<PyAny>,
}

impl RetrySession for PyCustomRetrySession {
    fn decide_should_retry(&mut self, request_info: RequestInfo) -> RetryDecision {
        Python::attach(|py| {
            let py_retry_session = self._inner.bind(py);
            let py_request_info = PyRequestInfo::from(&request_info);

            let result = py_retry_session
                .call_method1(intern!(py, "decide_should_retry"), (py_request_info,));

            match result {
                Ok(res) => match res.cast::<PyRetryDecision>() {
                    Ok(py_retry_decision) => RetryDecision::from(py_retry_decision.get()),
                    Err(err) => {
                        error!(
                            "Failed to extract 'PyRetryDecision'. \
                            Fallback action: 'DontRetry'. Reason: {}",
                            err
                        );
                        RetryDecision::DontRetry
                    }
                },
                Err(err) => {
                    error!(
                        "Failed to call decide_should_retry() on custom retry session. \
                        Fallback action: 'DontRetry'. Reason: {}",
                        err
                    );
                    RetryDecision::DontRetry
                }
            }
        })
    }

    fn reset(&mut self) {
        Python::attach(|py| {
            let obj = self._inner.bind(py);

            if let Err(err) = obj.call_method0(intern!(py, "reset")) {
                error!(
                    "Failed to call reset() on custom retry session. \
                    Reason: {}",
                    err
                );
            }
        })
    }
}

#[pyclass(name = "DefaultRetrySession", frozen)]
pub(crate) struct PyDefaultRetrySession {
    pub(crate) _inner: Mutex<DefaultRetrySession>,
}

#[pymethods]
impl PyDefaultRetrySession {
    #[new]
    fn py_new() -> Self {
        Self {
            _inner: Mutex::new(DefaultRetrySession::new()),
        }
    }

    fn decide_should_retry(&self, request_info: PyRequestInfo, py: Python<'_>) -> PyRetryDecision {
        let mut inner = self._inner.lock_py_attached(py).unwrap();
        inner
            .decide_should_retry(request_info.to_request_info())
            .into()
    }

    fn reset(&self, py: Python<'_>) {
        let mut inner = self._inner.lock_py_attached(py).unwrap();
        inner.reset();
    }
}

#[pyclass(name = "DowngradingConsistencyRetrySession", frozen)]
pub(crate) struct PyDowngradingConsistencyRetrySession {
    pub(crate) _inner: Mutex<DowngradingConsistencyRetrySession>,
}

#[pymethods]
impl PyDowngradingConsistencyRetrySession {
    #[new]
    fn py_new() -> Self {
        Self {
            _inner: Mutex::new(DowngradingConsistencyRetrySession::new()),
        }
    }

    fn decide_should_retry(&self, request_info: PyRequestInfo, py: Python<'_>) -> PyRetryDecision {
        let mut inner = self._inner.lock_py_attached(py).unwrap();
        inner
            .decide_should_retry(request_info.to_request_info())
            .into()
    }

    fn reset(&self, py: Python<'_>) {
        let mut inner = self._inner.lock_py_attached(py).unwrap();
        inner.reset();
    }
}

#[pyclass(name = "FallthroughRetrySession", frozen)]
pub(crate) struct PyFallthroughRetrySession {
    pub(crate) _inner: Arc<FallthroughRetrySession>,
}

#[pymethods]
impl PyFallthroughRetrySession {
    #[new]
    fn py_new() -> Self {
        Self {
            _inner: Arc::new(FallthroughRetrySession),
        }
    }

    #[allow(unused_variables)]
    fn decide_should_retry(&self, request_info: PyRequestInfo) -> PyRetryDecision {
        PyRetryDecision::DontRetry()
    }

    fn reset(&self) {}
}

#[allow(dead_code)]
#[derive(Debug)]
pub(crate) struct PyCustomRetryPolicy {
    pub(crate) _inner: Py<PyAny>,
}

impl RetryPolicy for PyCustomRetryPolicy {
    fn new_session(&self) -> Box<dyn RetrySession> {
        Python::attach(|py| -> Box<dyn RetrySession> {
            let policy = self._inner.bind(py);

            match policy.call_method0(intern!(py, "new_session")) {
                Ok(session) => Box::new(PyCustomRetrySession {
                    _inner: session.unbind(),
                }),
                Err(err) => {
                    error!(
                        "Failed to call new_session() on custom retry policy. \
                        Fallback action: 'DefaultRetrySession'. Reason: {}",
                        err
                    );
                    Box::new(DefaultRetrySession::new())
                }
            }
        })
    }
}

#[pyclass(name = "DefaultRetryPolicy", frozen)]
#[derive(Debug)]
pub(crate) struct PyDefaultRetryPolicy {
    pub(crate) _inner: Arc<DefaultRetryPolicy>,
}

#[pymethods]
impl PyDefaultRetryPolicy {
    #[new]
    fn py_new() -> Self {
        Self {
            _inner: Arc::new(DefaultRetryPolicy::new()),
        }
    }

    fn new_session(&self) -> PyDefaultRetrySession {
        PyDefaultRetrySession {
            _inner: Mutex::new(DefaultRetrySession::new()),
        }
    }
}

#[pyclass(name = "DowngradingConsistencyRetryPolicy", frozen)]
#[derive(Debug)]
pub(crate) struct PyDowngradingConsistencyRetryPolicy {
    pub(crate) _inner: Arc<DowngradingConsistencyRetryPolicy>,
}

#[pymethods]
impl PyDowngradingConsistencyRetryPolicy {
    #[new]
    fn py_new() -> Self {
        Self {
            _inner: Arc::new(DowngradingConsistencyRetryPolicy::new()),
        }
    }

    fn new_session(&self) -> PyDowngradingConsistencyRetrySession {
        PyDowngradingConsistencyRetrySession {
            _inner: Mutex::new(DowngradingConsistencyRetrySession::new()),
        }
    }
}

#[pyclass(name = "FallthroughRetryPolicy", frozen)]
#[derive(Debug)]
pub(crate) struct PyFallthroughRetryPolicy {
    pub(crate) _inner: Arc<FallthroughRetryPolicy>,
}

#[pymethods]
impl PyFallthroughRetryPolicy {
    #[new]
    fn py_new() -> Self {
        Self {
            _inner: Arc::new(FallthroughRetryPolicy::new()),
        }
    }

    fn new_session(&self) -> PyFallthroughRetrySession {
        PyFallthroughRetrySession {
            _inner: Arc::new(FallthroughRetrySession),
        }
    }
}

#[allow(dead_code)]
pub(crate) fn py_any_to_arc_retry_policy(rp: &Py<PyAny>, py: Python<'_>) -> Arc<dyn RetryPolicy> {
    let obj = rp.bind(py);
    if let Ok(policy) = obj.cast::<PyDefaultRetryPolicy>() {
        policy.get()._inner.clone()
    } else if let Ok(policy) = obj.cast::<PyDowngradingConsistencyRetryPolicy>() {
        policy.get()._inner.clone()
    } else if let Ok(policy) = obj.cast::<PyFallthroughRetryPolicy>() {
        policy.get()._inner.clone()
    } else {
        Arc::new(PyCustomRetryPolicy {
            _inner: rp.clone_ref(py),
        })
    }
}
