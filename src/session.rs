use std::sync::Arc;

use crate::RUNTIME;
use crate::batch::PyBatch;
use crate::deserialize::results::{Pager, PyPagingState, RequestResult, RowFactory};
use crate::errors::{
    DriverExecuteError, DriverPrepareError, DriverSchemaAgreementError,
    DriverStatementConversionError,
};
use crate::serialize::value_list::PyValueList;
use crate::statement::PyPreparedStatement;
use crate::statement::PyStatement;
use pyo3::prelude::*;
use pyo3::types::PyString;
use scylla::client::session::Session as ScyllaSession;
use scylla::response::query_result::QueryResult;
use scylla::statement::batch::BatchStatement;
use scylla::statement::prepared::PreparedStatement;
use scylla::statement::unprepared::Statement;
use scylla_cql::frame::request::query::{PagingState, PagingStateResponse};
use std::future::Future;

#[pyclass(frozen, skip_from_py_object)]
#[derive(Clone)]
pub(crate) struct Session {
    pub(crate) _inner: Arc<ScyllaSession>,
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
    ) -> Result<RequestResult, DriverExecuteError> {
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
                return Err(DriverExecuteError::paging_state_must_be_none_for_unpaged_execution());
            }

            self.execute_unpaged(statement, values, factory).await
        }
    }

    async fn prepare(
        &self,
        statement: ExecutableStatement,
    ) -> Result<PyPreparedStatement, DriverPrepareError> {
        match statement {
            ExecutableStatement::Unprepared(s) => self.scylla_prepare(s).await,
            ExecutableStatement::Prepared(_) => {
                Err(DriverPrepareError::cannot_prepare_prepared_statement())
            }
        }
    }

    #[pyo3(signature = (batch, /, *,  factory=None))]
    async fn batch(
        &self,
        batch: PyBatch,
        factory: Option<Py<RowFactory>>,
    ) -> Result<RequestResult, DriverExecuteError> {
        let result = self
            .session_spawn_on_runtime(async move |s| {
                s.batch(&batch._inner, batch.values)
                    .await
                    .map_err(DriverExecuteError::rust_driver_execution_error)
            })
            .await?;

        Ok(RequestResult::new(result, Pager::unpaged(), factory))
    }

    async fn await_schema_agreement(&self) -> Result<uuid::Uuid, DriverSchemaAgreementError> {
        let schema_version = self
            .session_spawn_on_runtime(async move |s| {
                s.await_schema_agreement()
                    .await
                    .map_err(DriverSchemaAgreementError::rust_driver_schema_agreement_error)
            })
            .await?;

        Ok(schema_version)
    }

    async fn check_schema_agreement(
        &self,
    ) -> Result<Option<uuid::Uuid>, DriverSchemaAgreementError> {
        let schema_version = self
            .session_spawn_on_runtime(async move |s| {
                s.check_schema_agreement()
                    .await
                    .map_err(DriverSchemaAgreementError::rust_driver_schema_agreement_error)
            })
            .await?;

        Ok(schema_version)
    }
}

impl Session {
    async fn execute_unpaged(
        &self,
        statement: ExecutableStatement,
        values: PyValueList,
        factory: Option<Py<RowFactory>>,
    ) -> Result<RequestResult, DriverExecuteError> {
        let result = self
            .session_spawn_on_runtime(async move |s| {
                match statement {
                    ExecutableStatement::Prepared(p) => s.execute_unpaged(&p, values).await,
                    ExecutableStatement::Unprepared(q) => s.query_unpaged(q, values).await,
                }
                .map_err(DriverExecuteError::rust_driver_execution_error)
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
    ) -> Result<RequestResult, DriverExecuteError> {
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

    async fn session_spawn_on_runtime<F, Fut, R, E>(&self, f: F) -> Result<R, E>
    where
        // closure: takes Arc<ScyllaSession> and returns a future
        F: FnOnce(Arc<ScyllaSession>) -> Fut + Send + 'static,
        // for spawn we need Send + 'static
        Fut: Future<Output = Result<R, E>> + Send + 'static,
        R: Send + 'static,
        // Error: Send + 'static, and also convertible from JoinError for better error handling
        E: From<tokio::task::JoinError> + Send + 'static,
    {
        let session_clone = Arc::clone(&self._inner);

        RUNTIME.spawn(async move { f(session_clone).await }).await?
    }

    async fn scylla_prepare(
        &self,
        statement: impl Into<Statement>,
    ) -> Result<PyPreparedStatement, DriverPrepareError> {
        match self._inner.prepare(statement).await {
            Ok(prepared) => Ok(PyPreparedStatement { _inner: prepared }),
            Err(err) => Err(DriverPrepareError::rust_driver_prepare_error(err)),
        }
    }

    pub(crate) async fn execute_single_page(
        &self,
        paging_state: PagingState,
        query_request: ExecutableStatement,
        values: PyValueList,
    ) -> Result<(QueryResult, PagingStateResponse), DriverExecuteError> {
        self.session_spawn_on_runtime(async move |s| {
            match query_request {
                ExecutableStatement::Prepared(p) => {
                    s.execute_single_page(&p, values, paging_state).await
                }
                ExecutableStatement::Unprepared(q) => {
                    s.query_single_page(q, values, paging_state).await
                }
            }
            .map_err(DriverExecuteError::rust_driver_execution_error)
        })
        .await
    }
}

#[derive(Clone)]
pub(crate) enum ExecutableStatement {
    Prepared(PreparedStatement),
    Unprepared(Statement),
}

impl<'py> FromPyObject<'_, 'py> for ExecutableStatement {
    type Error = DriverStatementConversionError;

    fn extract(obj: Borrowed<'_, 'py, PyAny>) -> Result<Self, Self::Error> {
        if let Ok(prepared) = obj.cast::<PyPreparedStatement>() {
            return Ok(ExecutableStatement::Prepared(prepared.get()._inner.clone()));
        }

        if let Ok(text) = obj.cast::<PyString>() {
            let text = text
                .to_str()
                .map_err(DriverStatementConversionError::statement_string_conversion_failed)?;
            return Ok(ExecutableStatement::Unprepared(text.into()));
        }

        if let Ok(statement) = obj.cast::<PyStatement>() {
            return Ok(ExecutableStatement::Unprepared(
                statement.get()._inner.clone(),
            ));
        }

        let got = obj
            .get_type()
            .name()
            .map(|name| name.to_string())
            .unwrap_or_else(|_| "<unknown type>".to_string());

        Err(DriverStatementConversionError::invalid_statement_type(got))
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
