use std::sync::Arc;

use crate::RUNTIME;
use crate::deserialize::results::{PyPagingState, QueryPager, RequestResult};
use crate::statement::PreparedStatement;
use pyo3::exceptions::PyRuntimeError;
use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;
use pyo3::types::PyString;
use scylla::statement;

use crate::statement::Statement;
use scylla::response::query_result::QueryResult;
use scylla::statement::unprepared;
use scylla_cql::frame::request::query::{PagingState, PagingStateResponse};

#[pyclass]
#[derive(Clone)]
pub(crate) struct Session {
    pub(crate) _inner: Arc<scylla::client::session::Session>,
}

#[pymethods]
impl Session {
    #[pyo3(signature = (request, paging_state=None, paged=true))]
    async fn execute(
        &self,
        request: Py<PyAny>,
        paging_state: Option<Py<PyPagingState>>,
        paged: bool,
    ) -> PyResult<RequestResult> {
        if paged {
            self.execute_paged(request, paging_state).await
        } else {
            self.execute_unpaged(request).await
        }
    }

    async fn prepare(&self, statement: Py<PyAny>) -> PyResult<PreparedStatement> {
        let query_request = ExecutableStatement::extract_request(statement)?;

        match query_request {
            ExecutableStatement::Unprepared(s) => self.scylla_prepare(s).await,
            ExecutableStatement::Prepared(_) => Err(PyErr::new::<PyTypeError, _>(
                "Cannot prepare a PreparedStatement; expected a str or Statement".to_string(),
            )),
        }
    }
}

impl Session {
    async fn execute_unpaged(&self, request: Py<PyAny>) -> PyResult<RequestResult> {
        let query_request = ExecutableStatement::extract_request(request)?;
        let result = self
            .session_spawn_on_runtime(async move |s| {
                match query_request {
                    ExecutableStatement::Prepared(p) => s.execute_unpaged(&p, &[]).await,
                    ExecutableStatement::Unprepared(q) => s.query_unpaged(q, &[]).await,
                }
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))
            })
            .await?;

        RequestResult::new(result, QueryPager::unpaged())
    }

    async fn execute_paged(
        &self,
        request: Py<PyAny>,
        paging_state: Option<Py<PyPagingState>>,
    ) -> PyResult<RequestResult> {
        let query_request = ExecutableStatement::extract_request(request)?;

        //Ensure the query is prepared so it can be efficiently reused while fetching pages.

        let prepared = match query_request {
            ExecutableStatement::Prepared(p) => p,
            ExecutableStatement::Unprepared(s) => self.scylla_prepare(s).await?._inner,
        };

        let paging_state = if let Some(state) = paging_state {
            Python::attach(|py| state.borrow(py).inner.clone())
        } else {
            PagingState::start()
        };

        let (result, paging_response) = self
            .execute_single_page(paging_state, prepared.clone())
            .await?;

        RequestResult::new(result, QueryPager::paged(paging_response, self, &prepared))
    }
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

    async fn scylla_prepare(
        &self,
        statement: impl Into<statement::Statement>,
    ) -> PyResult<PreparedStatement> {
        match self._inner.prepare(statement).await {
            Ok(prepared) => Ok(PreparedStatement { _inner: prepared }),
            Err(e) => Err(PyErr::new::<PyRuntimeError, _>(format!(
                "Failed to prepare statement: {}",
                e
            ))),
        }
    }

    pub(crate) async fn execute_single_page(
        &self,
        paging_state: PagingState,
        prepared: scylla::statement::prepared::PreparedStatement,
    ) -> Result<(QueryResult, PagingStateResponse), PyErr> {
        self.session_spawn_on_runtime(async move |s| {
            s.execute_single_page(&prepared, &[], paging_state)
                .await
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))
        })
        .await
    }
}

#[derive(Clone)]
pub(crate) enum ExecutableStatement {
    Prepared(statement::prepared::PreparedStatement),
    Unprepared(unprepared::Statement),
}

impl ExecutableStatement {
    fn extract_request(request: Py<PyAny>) -> PyResult<ExecutableStatement> {
        Python::attach(|py| {
            if let Ok(prepared) = request.extract::<Py<PreparedStatement>>(py) {
                return Ok(ExecutableStatement::Prepared(prepared.get()._inner.clone()));
            }

            if let Ok(text) = request.extract::<Py<PyString>>(py) {
                return Ok(ExecutableStatement::Unprepared(text.to_str(py)?.into()));
            }

            if let Ok(statement) = request.extract::<Py<Statement>>(py) {
                return Ok(ExecutableStatement::Unprepared(
                    statement.get()._inner.clone(),
                ));
            }

            Err(PyErr::new::<PyTypeError, _>(format!(
                "Invalid request type: expected str | Statement | PreparedStatement, got {}",
                request.into_bound(py).get_type().name()?
            )))
        })
    }
}

#[pymodule]
pub(crate) fn session(_py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<Session>()?;

    Ok(())
}
