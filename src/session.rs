use std::sync::Arc;

use crate::deserialize::results::RequestResult;
use crate::serialize::value_list::PyAnyWrapperValueList;
use crate::statement::PreparedStatement;
use pyo3::exceptions::PyRuntimeError;
use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;
use scylla::statement;

use crate::RUNTIME;
use crate::statement::Statement;
use pyo3::types::{PyDict, PyList, PyString, PyTuple};

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
    #[pyo3(signature = (request, values = None))]
    async fn execute(
        &self,
        request: Py<PyAny>,
        values: Option<Py<PyAny>>,
    ) -> PyResult<RequestResult> {
        let value_list = values.map(try_into_value_list).transpose()?;

        if let Ok(prepared) = Python::attach(|py| {
            let scylla_prepared = request.extract::<Py<PreparedStatement>>(py)?;
            Ok::<Py<PreparedStatement>, PyErr>(scylla_prepared)
        }) {
            let result = self
                .spawn_on_runtime(async move |s| {
                    let res = match value_list {
                        Some(row) => s.execute_unpaged(&prepared.get()._inner, row).await,
                        None => s.execute_unpaged(&prepared.get()._inner, &[]).await,
                    };

                    res.map_err(|e| PyRuntimeError::new_err(format!("Failed query_unpaged: {}", e)))
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
                .spawn_on_runtime(async move |s| {
                    let res = match value_list {
                        Some(row) => s.query_unpaged(statement.get()._inner.clone(), row).await,
                        None => s.query_unpaged(statement.get()._inner.clone(), &[]).await,
                    };

                    res.map_err(|e| PyRuntimeError::new_err(format!("Failed query_unpaged: {}", e)))
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
                .spawn_on_runtime(async move |s| {
                    let res = match value_list {
                        Some(row) => s.query_unpaged(text, row).await,
                        None => s.query_unpaged(text, &[]).await,
                    };

                    res.map_err(|e| PyRuntimeError::new_err(format!("Failed query_unpaged: {}", e)))
                })
                .await?; // Propagate error form closure
            return Ok(RequestResult {
                inner: Arc::new(result),
            });
        }
        Err(PyErr::new::<PyTypeError, _>("Invalid request type"))
    }

    async fn prepare(&self, statement: Py<PyAny>) -> PyResult<PreparedStatement> {
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
        Err(PyErr::new::<PyTypeError, _>("Invalid statement type"))
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

#[pymodule]
pub(crate) fn session(_py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<Session>()?;

    Ok(())
}
