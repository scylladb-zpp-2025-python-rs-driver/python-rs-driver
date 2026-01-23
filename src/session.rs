use std::sync::Arc;

use crate::deserialize::results::{PyPagingState, QueryPager, RequestResult};
use crate::serialize::value_list::PyAnyWrapperValueList;
use pyo3::exceptions::PyRuntimeError;
use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;
use scylla::statement;

use crate::RUNTIME;
use crate::statement::PreparedStatement;
use crate::statement::Statement;
use pyo3::types::{PyDict, PyList, PyString, PyTuple};
use scylla::response::query_result::QueryResult;
use scylla::statement::unprepared;
use scylla_cql::frame::request::query::{PagingState, PagingStateResponse};

#[pyclass]
#[derive(Clone)]
pub(crate) struct Session {
    pub(crate) _inner: Arc<scylla::client::session::Session>,
}

fn try_into_value_list(values: Py<PyAny>) -> PyResult<PyAnyWrapperValueList> {
    Python::attach(|py| {
        let val: Bound<'_, PyAny> = values.into_bound(py);

        if val.is_instance_of::<PyList>()
            || val.is_instance_of::<PyTuple>()
            || val.is_instance_of::<PyDict>()
        {
            let is_empty = is_empty_row(&val);
            return Ok(PyAnyWrapperValueList {
                inner: val.unbind(),
                is_empty,
            });
        }

        let python_type_name = val.get_type().name()?;
        let python_type_name = python_type_name.extract::<&str>()?;

        Err(PyErr::new::<PyTypeError, _>(format!(
            "Invalid row type: got {}, expected Python tuple, list or dict",
            python_type_name
        )))
    })
}

fn is_empty_row(row: &Bound<'_, PyAny>) -> bool {
    if row.is_none() {
        return true;
    }

    row.len().map(|len| len == 0).unwrap_or(false)
}

#[pymethods]
impl Session {
    #[pyo3(signature = (request, values=None, paging_state=None, paged=true))]
    async fn execute(
        &self,
        request: Py<PyAny>,
        values: Option<Py<PyAny>>,
        paging_state: Option<Py<PyPagingState>>,
        paged: bool,
    ) -> PyResult<RequestResult> {
        let value_list = values.map(try_into_value_list).transpose()?;

        if paged {
            self.execute_paged(request, paging_state, value_list).await
        } else {
            self.execute_unpaged(request, value_list).await
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
    async fn execute_unpaged(
        &self,
        request: Py<PyAny>,
        value_list: Option<PyAnyWrapperValueList>,
    ) -> PyResult<RequestResult> {
        let query_request = ExecutableStatement::extract_request(request)?;
        let result = self
            .session_spawn_on_runtime(async move |s| {
                match query_request {
                    ExecutableStatement::Prepared(p) => match value_list {
                        Some(row) => s.execute_unpaged(&p, row).await,
                        None => s.execute_unpaged(&p, &[]).await,
                    },
                    ExecutableStatement::Unprepared(q) => match value_list {
                        Some(row) => s.query_unpaged(q, row).await,
                        None => s.query_unpaged(q, &[]).await,
                    },
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
        value_list: Option<PyAnyWrapperValueList>,
    ) -> PyResult<RequestResult> {
        let query_request = ExecutableStatement::extract_request(request)?;

        let paging_state = if let Some(state) = paging_state {
            Python::attach(|py| state.borrow(py).inner.clone())
        } else {
            PagingState::start()
        };

        let (result, paging_response) = self
            .execute_single_page(paging_state, query_request.clone(), value_list.clone())
            .await?;

        RequestResult::new(result, QueryPager::paged(paging_response, self, query_request, value_list))
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
        query_request: ExecutableStatement,
        value_list: Option<PyAnyWrapperValueList>,
    ) -> Result<(QueryResult, PagingStateResponse), PyErr> {
        self.session_spawn_on_runtime(async move |s| {
            match query_request {
                ExecutableStatement::Prepared(p) => match value_list {
                    Some(row) => s.execute_single_page(&p, row, paging_state).await,
                    None => s.execute_single_page(&p, &[], paging_state).await,
                },
                ExecutableStatement::Unprepared(q) => match value_list {
                    Some(row) => s.query_single_page(q, row, paging_state).await,
                    None => s.query_single_page(q, &[], paging_state).await,
                },
            }
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
