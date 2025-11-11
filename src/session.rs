use std::fmt::Write;
use std::sync::Arc;

use crate::RUNTIME;
use crate::statements::PyPreparedStatement;
use crate::writers::SerializationBuffer;
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::PyString;
use scylla::value::Row;
use scylla_cql::frame::request::query::PagingState;

#[pyclass(module = "scylla._rust.session")]
pub(crate) struct Session {
    pub(crate) _inner: Arc<scylla::client::session::Session>,
}

#[pymethods]
impl Session {
    async fn _execute_raw_bytes(
        &self,
        prepared_statement: PyPreparedStatement,
        buffer: SerializationBuffer,
    ) -> PyResult<RequestResult> {
        let prepared = Arc::clone(&prepared_statement._inner);
        let session_clone = Arc::clone(&self._inner);
        let serialized_values = buffer.get_serialized_values();

        let (result, _) = RUNTIME
            .spawn(async move {
                session_clone
                    .execute(&prepared, &serialized_values, None, PagingState::start())
                    .await
                    .map_err(|e| PyRuntimeError::new_err(format!("Failed to execute query: {}", e)))
            })
            .await
            .expect("Driver should not panic")?;
        Ok(RequestResult { inner: result })
    }
    async fn execute(&self, request: Py<PyString>) -> PyResult<RequestResult> {
        let request_string = Python::with_gil(|py| request.to_str(py))?.to_string();
        let session_clone = Arc::clone(&self._inner);
        let result = RUNTIME
            .spawn(async move {
                session_clone
                    .query_unpaged(request_string, &[])
                    .await
                    .map_err(|e| {
                        PyRuntimeError::new_err(format!("Failed to deserialize metadata: {}", e))
                    })
            })
            .await
            .expect("Driver should not panic")?;
        return Ok(RequestResult { inner: result });
    }

    async fn prepare(&self, request: Py<PyString>) -> PyResult<PyPreparedStatement> {
        let request_string = Python::with_gil(|py| request.to_str(py))?.to_string();
        let session_clone = Arc::clone(&self._inner);
        let result = RUNTIME
            .spawn(async move {
                session_clone.prepare(request_string).await.map_err(|e| {
                    PyRuntimeError::new_err(format!("Failed to deserialize metadata: {}", e))
                })
            })
            .await
            .expect("Driver should not panic")?;
        Ok(PyPreparedStatement {
            _inner: Arc::new(result),
        })
    }
}

#[pyclass]
pub(crate) struct RequestResult {
    pub(crate) inner: scylla::response::query_result::QueryResult,
}

#[pymethods]
impl RequestResult {
    fn __str__<'s, 'gil>(&'s mut self, py: Python<'gil>) -> PyResult<Bound<'gil, PyString>> {
        let mut result = String::new();
        let rows_result = match self.inner.clone().into_rows_result() {
            Ok(r) => r,
            Err(e) => return Ok(PyString::new(py, &format!("non-rows result: {}", e))),
        };
        for r in rows_result.rows::<Row>().map_err(|e| {
            PyRuntimeError::new_err(format!("Failed to deserialize metadata: {}", e))
        })? {
            let row = match r {
                Ok(r) => r,
                Err(e) => {
                    return Err(PyRuntimeError::new_err(format!(
                        "Failed to deserialize row: {}",
                        e
                    )));
                }
            };
            write!(result, "|").unwrap();
            for col in row.columns {
                match col {
                    Some(c) => write!(result, "{}", c).unwrap(),
                    None => write!(result, "null").unwrap(),
                };
                write!(result, "|").unwrap();
            }
            write!(result, "\n").unwrap();
        }
        return Ok(PyString::new(py, &result));
    }
}

#[pymodule]
pub(crate) fn session(_py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<Session>()?;
    module.add_class::<RequestResult>()?;

    Ok(())
}
