use std::sync::Arc;

use crate::deserialize::results::RequestResult;
use crate::serialize::value_list::PyAnyWrapperValueList;
use pyo3::exceptions::PyRuntimeError;
use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;
use scylla::statement;

use crate::RUNTIME;
use crate::statement::PreparedStatement;
use crate::statement::Statement;
use pyo3::types::{PyDict, PyList, PyString, PyTuple};
use scylla::statement::unprepared;

#[pyclass]
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
    #[pyo3(signature = (statement, values = None))]
    async fn execute(
        &self,
        statement: ExecutableStatement,
        values: Option<Py<PyAny>>,
    ) -> PyResult<RequestResult> {
        let value_list = values.map(try_into_value_list).transpose()?;

        let result = self
            .session_spawn_on_runtime(async move |s| {
                match statement {
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
