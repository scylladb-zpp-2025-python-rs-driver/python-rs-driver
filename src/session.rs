use std::sync::Arc;

use crate::deserialize::results::RequestResult;
use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;
use pyo3::types::PyString;
use scylla::statement;

use crate::RUNTIME;
use crate::errors::ExecutionError;
use crate::errors::bad_query_err;
use crate::statement::PreparedStatement;
use crate::statement::Statement;

#[pyclass]
pub(crate) struct Session {
    pub(crate) _inner: Arc<scylla::client::session::Session>,
}

#[pymethods]
impl Session {
    async fn execute(&self, request: Py<PyAny>) -> Result<RequestResult, ExecutionError> {
        if let Ok(prepared) = Python::attach(|py| {
            let scylla_prepared = request.extract::<Py<PreparedStatement>>(py)?;
            Ok::<Py<PreparedStatement>, PyErr>(scylla_prepared)
        }) {
            let result = self
                .session_spawn_on_runtime(async move |s| {
                    s.execute_unpaged(&prepared.get()._inner, &[])
                        .await
                        .map_err(|e| {
                            ExecutionError::Runtime(format!("Failed execute_unpaged: {}", e))
                        })
                })
                .await?; // Propagate error form closure
            return Ok(RequestResult {
                inner: Arc::new(result),
            });
        }

        if let Ok(statement) = Python::attach(|py| {
            let scylla_statement = request.extract::<Py<Statement>>(py)?;
            Ok::<Py<Statement>, PyErr>(scylla_statement)
        }) {
            let result = self
                .session_spawn_on_runtime(async move |s| {
                    s.query_unpaged(statement.get()._inner.clone(), &[])
                        .await
                        .map_err(|e| {
                            ExecutionError::Runtime(format!("Failed query_unpaged: {}", e))
                        })
                })
                .await?; // Propagate error from closure
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
                        ExecutionError::Runtime(format!("Failed query_unpaged: {}", e))
                    })
                })
                .await?; // Propagate error from closure
            return Ok(RequestResult {
                inner: Arc::new(result),
            });
        }
        Err(bad_query_err(PyTypeError::new_err("Invalid request type")))
    }

    async fn prepare(&self, statement: Py<PyAny>) -> Result<PreparedStatement, ExecutionError> {
        if let Ok(statement) = Python::attach(|py| {
            let scylla_statement = statement.extract::<Py<Statement>>(py)?;
            Ok::<Py<Statement>, PyErr>(scylla_statement)
        }) {
            let scylla_statement = statement.get()._inner.clone();
            return self.scylla_prepare(scylla_statement).await;
        }
        if let Ok(text) = Python::attach(|py| statement.extract::<String>(py)) {
            return self.scylla_prepare(text).await;
        }
        Err(bad_query_err(PyTypeError::new_err(
            "Invalid statement type",
        )))
    }
}

impl Session {
    async fn session_spawn_on_runtime<F, Fut, R, E>(&self, f: F) -> Result<R, E>
    where
        // closure: takes Arc<scylla::client::session::Session> and returns a future
        F: FnOnce(Arc<scylla::client::session::Session>) -> Fut + Send + 'static,
        // for spawn we need Send + 'static
        Fut: Future<Output = Result<R, E>> + Send + 'static,
        R: Send + 'static,
        E: Send + 'static,
    {
        let session_clone = Arc::clone(&self._inner);

        RUNTIME
            .spawn(async move { f(session_clone).await })
            .await
            .expect("Runtime failed to spawn task") // It's okay to panic here
    }

    async fn scylla_prepare(
        &self,
        statement: impl Into<statement::Statement>,
    ) -> Result<PreparedStatement, ExecutionError> {
        self._inner
            .prepare(statement)
            .await
            .map(|prepared| PreparedStatement { _inner: prepared })
            .map_err(|e| ExecutionError::BadQuery(format!("prepare failed: {e}")))
    }
}

#[pymodule]
pub(crate) fn session(_py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<Session>()?;

    Ok(())
}
