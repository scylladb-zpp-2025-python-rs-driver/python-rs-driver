use std::sync::Arc;

use crate::deserialize::results::RequestResult;
use pyo3::exceptions::PyRuntimeError;
use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;
use pyo3::types::PyString;

use crate::RUNTIME;
use crate::statement::PreparedStatement;

#[pyclass]
pub(crate) struct Session {
    pub(crate) _inner: Arc<scylla::client::session::Session>,
}

#[pymethods]
impl Session {
    async fn execute(&self, request: Py<PyAny>) -> PyResult<RequestResult> {
        if let Ok(prepared) = Python::attach(|py| {
            let scylla_prepared = request.extract::<Py<PreparedStatement>>(py)?;
            Ok::<Py<PreparedStatement>, PyErr>(scylla_prepared)
        }) {
            let result = self
                .session_spawn_on_runtime(async move |s| {
                    s.execute_unpaged(&prepared.get()._inner, &[])
                        .await
                        .map_err(|e| {
                            PyRuntimeError::new_err(format!("Failed execute_unpaged: {}", e))
                        })
                })
                .await?; // Propagate error form closure
            return Ok(RequestResult {
                inner: Arc::new(result),
            });
        }

        if let Ok(text) = Python::attach(|py| {
            let text = request.extract::<Py<PyString>>(py)?;
            Ok::<String, PyErr>(text.to_string())
        }) {
            let result = self
                .session_spawn_on_runtime(async move |s| {
                    s.query_unpaged(text, &[]).await.map_err(|e| {
                        PyRuntimeError::new_err(format!("Failed query_unpaged: {}", e))
                    })
                })
                .await?; // Propagate error form closure
            return Ok(RequestResult {
                inner: Arc::new(result),
            });
        }
        Err(PyErr::new::<PyTypeError, _>("Invalid request type"))
    }

    async fn prepare(&self, statement: String) -> PyResult<PreparedStatement> {
        let session_clone = std::sync::Arc::clone(&self._inner);
        match session_clone.prepare(statement).await {
            Ok(prepared) => Ok(PreparedStatement { _inner: prepared }),
            Err(e) => Err(PyErr::new::<PyRuntimeError, _>(format!(
                "Failed to prepare statement: {}",
                e
            ))),
        }
    }
}

impl Session {
    async fn session_spawn_on_runtime<F, Fut, R>(&self, f: F) -> PyResult<R>
    where
        // closure: takes Arc<scylla::client::session::Session> and returns a future
        F: FnOnce(Arc<scylla::client::session::Session>) -> Fut + Send + 'static,
        // for spawn we need Send + 'static
        Fut: Future<Output = PyResult<R>> + Send + 'static,
        R: Send + 'static,
    {
        let session_clone = Arc::clone(&self._inner);

        RUNTIME
            .spawn(async move { f(session_clone).await })
            .await
            .expect("Runtime failed to spawn task") // It's okay to panic here
    }
}

#[pymodule]
pub(crate) fn session(_py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<Session>()?;

    Ok(())
}
