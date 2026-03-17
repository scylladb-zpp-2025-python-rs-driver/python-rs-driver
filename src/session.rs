use std::sync::Arc;

use crate::RUNTIME;
use crate::batch::PyBatch;
use crate::deserialize::results::{Pager, PyPagingState, RequestResult, RowFactory};
use crate::serialize::value_list::PyValueList;
use crate::statement::PreparedStatement;
use crate::statement::Statement;
use pyo3::exceptions::PyTypeError;
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::PyString;
use scylla::response::query_result::QueryResult;
use scylla::statement;
use scylla::statement::batch::BatchStatement;
use scylla::statement::unprepared;
use scylla_cql::frame::request::query::{PagingState, PagingStateResponse};

#[pyclass(frozen, skip_from_py_object)]
#[derive(Clone)]
pub(crate) struct Session {
    pub(crate) _inner: Arc<scylla::client::session::Session>,
}

#[pymethods]
impl Session {
    #[pyo3(signature = (statement, values=None, /, *, factory=None, paging_state=None, paged=true))]
    async fn execute(
        &self,
        statement: ExecutableStatement,
        values: Option<PyValueList>,
        factory: Option<Py<RowFactory>>,
        paging_state: Option<Py<PyPagingState>>,
        paged: bool,
    ) -> PyResult<RequestResult> {
        // Why not accept PyValueList instead of Option<PyValueList>?
        // It would require us to use `Default::default` as default value in
        // `pyo3(signature = ...)`, and thus use `text_signature` as well
        // to keep signature usable for Python users. I think it is cleaner
        // to `unwrap_or_default()` here.
        let values = values.unwrap_or_default();
        if paged {
            self.execute_paged(statement, paging_state, values, factory)
                .await
        } else {
            if paging_state.is_some() {
                return Err(PyErr::new::<PyValueError, _>(
                    "paging_state must be None for unpaged execution".to_string(),
                ));
            }

            self.execute_unpaged(statement, values, factory).await
        }
    }

    async fn prepare(&self, statement: ExecutableStatement) -> PyResult<PreparedStatement> {
        match statement {
            ExecutableStatement::Unprepared(s) => self.scylla_prepare(s).await,
            ExecutableStatement::Prepared(_) => Err(PyErr::new::<PyTypeError, _>(
                "Cannot prepare a PreparedStatement; expected a str or Statement".to_string(),
            )),
        }
    }

    #[pyo3(signature = (batch, /, *,  factory=None))]
    async fn batch(
        &self,
        batch: PyBatch,
        factory: Option<Py<RowFactory>>,
    ) -> PyResult<RequestResult> {
        let result = self
            .session_spawn_on_runtime(async move |s| {
                s.batch(&batch._inner, batch.values)
                    .await
                    .map_err(|e| PyRuntimeError::new_err(e.to_string()))
            })
            .await?;

        Ok(RequestResult::new(result, Pager::unpaged(), factory))
    }
}

impl Session {
    async fn execute_unpaged(
        &self,
        statement: ExecutableStatement,
        values: PyValueList,
        factory: Option<Py<RowFactory>>,
    ) -> PyResult<RequestResult> {
        let result = self
            .session_spawn_on_runtime(async move |s| {
                match statement {
                    ExecutableStatement::Prepared(p) => s.execute_unpaged(&p, values).await,
                    ExecutableStatement::Unprepared(q) => s.query_unpaged(q, values).await,
                }
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))
            })
            .await?;

        Ok(RequestResult::new(result, Pager::unpaged(), factory))
    }

    async fn execute_paged(
        &self,
        statement: ExecutableStatement,
        paging_state: Option<Py<PyPagingState>>,
        values: PyValueList,
        factory: Option<Py<RowFactory>>,
    ) -> PyResult<RequestResult> {
        let paging_state = if let Some(state) = paging_state {
            Python::attach(|py| state.borrow(py).inner.clone())
        } else {
            PagingState::start()
        };

        let (result, paging_response) = self
            .execute_single_page(paging_state, statement.clone(), values.clone())
            .await?;

        Ok(RequestResult::new(
            result,
            Pager::paged(paging_response, self.clone(), statement, values),
            factory,
        ))
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
            Ok(prepared) => Ok(PreparedStatement::new(prepared, false)),
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
        values: PyValueList,
    ) -> Result<(QueryResult, PagingStateResponse), PyErr> {
        self.session_spawn_on_runtime(async move |s| {
            match query_request {
                ExecutableStatement::Prepared(p) => {
                    s.execute_single_page(&p, values, paging_state).await
                }
                ExecutableStatement::Unprepared(q) => {
                    s.query_single_page(q, values, paging_state).await
                }
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

impl<'py> FromPyObject<'_, 'py> for ExecutableStatement {
    type Error = PyErr;

    fn extract(obj: Borrowed<'_, 'py, PyAny>) -> Result<Self, Self::Error> {
        if let Ok(prepared) = obj.cast::<PreparedStatement>() {
            return Ok(ExecutableStatement::Prepared(prepared.get()._inner.clone()));
        }

        if let Ok(text) = obj.cast::<PyString>() {
            return Ok(ExecutableStatement::Unprepared(text.to_str()?.into()));
        }

        if let Ok(statement) = obj.cast::<Statement>() {
            return Ok(ExecutableStatement::Unprepared(
                statement.get()._inner.clone(),
            ));
        }

        Err(PyErr::new::<PyTypeError, _>(format!(
            "Invalid statement type: expected str | Statement | PreparedStatement, got {}",
            obj.get_type().name()?
        )))
    }
}

impl From<ExecutableStatement> for BatchStatement {
    fn from(s: ExecutableStatement) -> Self {
        match s {
            ExecutableStatement::Prepared(p) => BatchStatement::PreparedStatement(p),
            ExecutableStatement::Unprepared(u) => BatchStatement::Query(u),
        }
    }
}

#[pymodule]
pub(crate) fn session(_py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<Session>()?;

    Ok(())
}
