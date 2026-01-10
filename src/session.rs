use std::sync::Arc;

use crate::RUNTIME;
use crate::deserialize::results::{PagingRequestResult, PyPagingState, RequestResult};
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
    async fn execute(&self, request: Py<PyAny>) -> PyResult<RequestResult> {
        let query_request = QueryRequest::extract_request(request)?;
        let result = self
            .session_spawn_on_runtime(async move |s| {
                match query_request {
                    QueryRequest::Prepared(p) => s.execute_unpaged(&p, &[]).await,
                    QueryRequest::Text(q) => s.query_unpaged(q.as_str(), &[]).await,
                    QueryRequest::Statement(q) => s.query_unpaged(q, &[]).await,
                }
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))
            })
            .await?;

        Ok(RequestResult {
            inner: Arc::new(result),
        })
    }

    // TODO:
    // Choose one paging approach. There are currently two draft implementations.
    // We should either merge their responsibilities into a single consistent API (possible only with execute_single_page)
    // or keep only and leave out some functionalities
    #[pyo3(signature = (request, paging_state=None, page_size=None))]
    async fn execute_paged(
        &self,
        request: Py<PyAny>,
        paging_state: Option<Py<PyPagingState>>,
        page_size: Option<i32>,
    ) -> PyResult<PagingRequestResult> {
        let query_request = QueryRequest::extract_request(request)?;

        //Ensure the query is prepared so it can be efficiently reused while fetching pages.
        let mut prepared = match query_request {
            QueryRequest::Prepared(p) => p,

            QueryRequest::Text(sql) => self.scylla_prepare(sql).await?._inner,
            QueryRequest::Statement(s) => self.scylla_prepare(s).await?._inner,
        };

        let paging_state = if let Some(state) = paging_state {
            Python::attach(|py| state.borrow(py).inner.clone())
        } else {
            PagingState::start()
        };

        if let Some(page_size) = page_size {
            prepared.set_page_size(page_size)
        }

        let (result, paging_response) = self
            .execute_single_page(paging_state, prepared.clone())
            .await?;

        Ok(PagingRequestResult::new(
            paging_response,
            self.clone(),
            prepared.clone(),
            result,
        ))
    }

    async fn prepare(&self, statement: Py<PyAny>) -> PyResult<PreparedStatement> {
        let query_request = QueryRequest::extract_request(statement)?;

        match query_request {
            QueryRequest::Statement(s) => self.scylla_prepare(s).await,
            QueryRequest::Text(sql) => self.scylla_prepare(sql).await,
            _ => Err(PyErr::new::<PyTypeError, _>("Invalid statement type")),
        }
    }
}

#[derive(Clone)]
pub(crate) enum QueryRequest {
    Prepared(statement::prepared::PreparedStatement),
    Text(String),
    Statement(unprepared::Statement),
}

impl QueryRequest {
    fn extract_request(request: Py<PyAny>) -> PyResult<QueryRequest> {
        Python::attach(|py| {
            if let Ok(prepared) = request.extract::<Py<PreparedStatement>>(py) {
                return Ok(QueryRequest::Prepared(prepared.get()._inner.clone()));
            }

            if let Ok(text) = request.extract::<Py<PyString>>(py) {
                return Ok(QueryRequest::Text(text.to_string()));
            }

            if let Ok(statement) = request.extract::<Py<Statement>>(py) {
                return Ok(QueryRequest::Statement(statement.get()._inner.clone()));
            }

            Err(PyErr::new::<PyTypeError, _>("Invalid request type"))
        })
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

#[pymodule]
pub(crate) fn session(_py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<Session>()?;

    Ok(())
}
