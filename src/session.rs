use std::sync::Arc;

use crate::deserialize::results::RequestResult;
use crate::serialize::value_list::PyValueList;
use crate::statement::PreparedStatement;
use pyo3::exceptions::PyRuntimeError;
use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;
use scylla::statement;

use crate::RUNTIME;
use crate::statement::Statement;
use pyo3::types::PyString;
use scylla::statement::unprepared;

#[pyclass]
pub(crate) struct Session {
    pub(crate) _inner: Arc<scylla::client::session::Session>,
}

#[pymethods]
impl Session {
    #[pyo3(signature = (statement, values = None))]
    async fn execute(
        &self,
        statement: ExecutableStatement,
        values: Option<PyValueList>,
    ) -> PyResult<RequestResult> {
        // Why not accept PyValueList instead of Option<PyValueList>?
        // It would require us to use `Default::default` as default value in
        // `pyo3(signature = ...)`, and thus use `text_signature` as well
        // to keep signature usable for Python users. I think it is cleaner
        // to `unwrap_or_default()` here.
        let values = values.unwrap_or_default();
        let result = self
            .spawn_on_runtime(async move |s| {
                match statement {
                    ExecutableStatement::Prepared(p) => s.execute_unpaged(&p, values).await,
                    ExecutableStatement::Unprepared(q) => s.query_unpaged(q, values).await,
                }
                .map_err(|e| PyRuntimeError::new_err(e.to_string()))
            })
            .await?;

        Ok(RequestResult {
            inner: Arc::new(result),
        })
    }

    async fn prepare(&self, statement: ExecutableStatement) -> PyResult<PreparedStatement> {
        match statement {
            ExecutableStatement::Unprepared(s) => self.scylla_prepare(s).await,
            ExecutableStatement::Prepared(_) => Err(PyErr::new::<PyTypeError, _>(
                "Cannot prepare a PreparedStatement; expected a str or Statement".to_string(),
            )),
        }
    }
}

impl Session {
    async fn spawn_on_runtime<F, Fut, R>(&self, f: F) -> PyResult<R>
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
}

#[derive(Clone)]
pub(crate) enum ExecutableStatement {
    Prepared(statement::prepared::PreparedStatement),
    Unprepared(unprepared::Statement),
}

impl<'py> FromPyObject<'_, 'py> for ExecutableStatement {
    type Error = PyErr;

    fn extract(obj: Borrowed<'_, 'py, PyAny>) -> Result<Self, Self::Error> {
        if let Ok(prepared) = obj.extract::<Bound<'py, PreparedStatement>>() {
            return Ok(ExecutableStatement::Prepared(prepared.get()._inner.clone()));
        }

        if let Ok(text) = obj.extract::<Bound<'py, PyString>>() {
            return Ok(ExecutableStatement::Unprepared(text.to_str()?.into()));
        }

        if let Ok(statement) = obj.extract::<Bound<'py, Statement>>() {
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

#[pymodule]
pub(crate) fn session(_py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<Session>()?;

    Ok(())
}
