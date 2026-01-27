use std::future::Future;
use std::sync::Arc;

use crate::deserialize::results::RequestResult;
use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;
use pyo3::types::PyString;
use scylla::statement;

use crate::RUNTIME;
use crate::errors::{DriverExecutionError, ExecutionOp, ExecutionSource};
use crate::statement::PreparedStatement;
use crate::statement::Statement;

#[pyclass]
pub(crate) struct Session {
    pub(crate) _inner: Arc<scylla::client::session::Session>,
}

#[pymethods]
impl Session {
    async fn execute(&self, request: Py<PyAny>) -> Result<RequestResult, DriverExecutionError> {
        // Try PreparedStatement first
        if let Ok(prepared) = Python::attach(|py| {
            let scylla_prepared = request.extract::<Py<PreparedStatement>>(py)?;
            Ok::<Py<PreparedStatement>, PyErr>(scylla_prepared)
        }) {
            let result = self
                .session_spawn_on_runtime(async move |s| {
                    s.execute_unpaged(&prepared.get()._inner, &[])
                        .await
                        .map_err(|e| {
                            DriverExecutionError::runtime(
                                ExecutionOp::ExecuteUnpaged,
                                Some(ExecutionSource::RustErr(Box::new(e))),
                                "execute_unpaged failed",
                            )
                        })
                })
                .await?;
            return Ok(RequestResult {
                inner: Arc::new(result),
            });
        }

        // Then try Statement
        if let Ok(statement) = Python::attach(|py| {
            let scylla_statement = request.extract::<Py<Statement>>(py)?;
            Ok::<Py<Statement>, PyErr>(scylla_statement)
        }) {
            // Clone CQL for error reporting
            let cql = statement.get()._inner.contents.clone();

            let result = self
                .session_spawn_on_runtime(async move |s| {
                    s.query_unpaged(statement.get()._inner.clone(), &[])
                        .await
                        .map_err(|e| {
                            DriverExecutionError::runtime(
                                ExecutionOp::QueryUnpaged,
                                Some(ExecutionSource::RustErr(Box::new(e))),
                                "query_unpaged failed",
                            )
                            .with_cql(cql)
                        })
                })
                .await?;
            return Ok(RequestResult {
                inner: Arc::new(result),
            });
        }

        // Finally try raw CQL string
        if let Ok(text) = Python::attach(|py| {
            let text = request.extract::<Py<PyString>>(py)?;
            Ok::<String, PyErr>(text.to_string())
        }) {
            let cql = text.clone(); // Clone for error reporting

            let result = self
                .session_spawn_on_runtime(async move |s| {
                    s.query_unpaged(text, &[]).await.map_err(|e| {
                        DriverExecutionError::runtime(
                            ExecutionOp::QueryUnpaged,
                            Some(ExecutionSource::RustErr(Box::new(e))),
                            "query_unpaged failed",
                        )
                        .with_cql(cql)
                    })
                })
                .await?;
            return Ok(RequestResult {
                inner: Arc::new(result),
            });
        }

        // If none matched, invalid request type
        let request_type = Python::attach(|py| {
            request
                .bind(py)
                .get_type()
                .name()
                .map(|n| n.to_string_lossy().into_owned())
                .unwrap_or_else(|_| "Unknown".to_string())
        });

        let cause = PyTypeError::new_err(
            "Invalid request type (expected PreparedStatement, Statement, or str)",
        );

        Err(DriverExecutionError::bad_query(
            ExecutionOp::ExecuteUnpaged,
            Some(ExecutionSource::PyErr(cause)),
            "Invalid request type",
        )
        .with_request_type(request_type))
    }

    async fn prepare(
        &self,
        statement: Py<PyAny>,
    ) -> Result<PreparedStatement, DriverExecutionError> {
        // Try Statement first
        if let Ok(statement) = Python::attach(|py| {
            let scylla_statement = statement.extract::<Py<Statement>>(py)?;
            Ok::<Py<Statement>, PyErr>(scylla_statement)
        }) {
            let cql = statement.get()._inner.contents.clone();
            let scylla_statement = statement.get()._inner.clone();
            return self.scylla_prepare(scylla_statement, Some(cql)).await;
        }

        // Then try raw CQL string
        if let Ok(text) = Python::attach(|py| statement.extract::<String>(py)) {
            let cql = text.clone();
            return self.scylla_prepare(text, Some(cql)).await;
        }

        // If none matched, invalid statement type
        let statement_type = Python::attach(|py| {
            statement
                .bind(py)
                .get_type()
                .name()
                .map(|n| n.to_string_lossy().into_owned())
                .unwrap_or_else(|_| "Unknown".to_string())
        });

        let cause = PyTypeError::new_err("Invalid statement type (expected Statement or str)");
        Err(DriverExecutionError::bad_query(
            ExecutionOp::Prepare,
            Some(ExecutionSource::PyErr(cause)),
            "Invalid statement type",
        )
        .with_request_type(statement_type))
    }
}

impl Session {
    async fn session_spawn_on_runtime<F, Fut, R>(&self, f: F) -> Result<R, DriverExecutionError>
    where
        // closure: takes Arc<scylla::client::session::Session> and returns a future
        F: FnOnce(Arc<scylla::client::session::Session>) -> Fut + Send + 'static,
        // for spawn we need Send + 'static
        Fut: Future<Output = Result<R, DriverExecutionError>> + Send + 'static,
        R: Send + 'static,
    {
        let session_clone = Arc::clone(&self._inner);

        RUNTIME
            .spawn(async move { f(session_clone).await })
            .await
            .map_err(|e| {
                DriverExecutionError::runtime(
                    ExecutionOp::SpawnJoin,
                    Some(ExecutionSource::RustErr(Box::new(e))),
                    "tokio join error while running request",
                )
            })?
    }

    async fn scylla_prepare(
        &self,
        statement: impl Into<statement::Statement>,
        cql_for_ctx: Option<String>,
    ) -> Result<PreparedStatement, DriverExecutionError> {
        self._inner
            .prepare(statement)
            .await
            .map(|prepared| PreparedStatement { _inner: prepared })
            .map_err(|e| {
                let mut err = DriverExecutionError::bad_query(
                    ExecutionOp::Prepare,
                    Some(ExecutionSource::RustErr(Box::new(e))),
                    "prepare failed",
                );
                if let Some(cql) = cql_for_ctx {
                    err = err.with_cql(cql);
                }
                err
            })
    }
}

#[pymodule]
pub(crate) fn session(_py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<Session>()?;

    Ok(())
}
