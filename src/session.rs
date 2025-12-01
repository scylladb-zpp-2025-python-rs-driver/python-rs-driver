use crate::cqlvalue_row::RustCqlRow;
use crate::cqlvalue_to_py::cql_value_to_py;

use std::fmt::Write;
use std::sync::Arc;

use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyString};
use scylla::value::Row;

use crate::RUNTIME;

#[pyclass]
pub(crate) struct Session {
    pub(crate) _inner: Arc<scylla::client::session::Session>,
}

#[pymethods]
impl Session {
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
        Ok(RequestResult { inner: result })
    }
}

#[pyclass]
pub(crate) struct RequestResult {
    pub(crate) inner: scylla::response::query_result::QueryResult,
}

#[pymethods]
impl RequestResult {
    fn __str__<'gil>(&mut self, py: Python<'gil>) -> PyResult<Bound<'gil, PyString>> {
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
            writeln!(result).unwrap();
        }
        Ok(PyString::new(py, &result))
    }

    // Convert all rows to a Python list of dictionaries
    pub fn rows_as_dicts(&self, py: Python<'_>) -> PyResult<PyObject> {
        let rows_result = self
            .inner
            .clone()
            .into_rows_result()
            .map_err(|e| PyRuntimeError::new_err(format!("non-rows result: {e}")))?;

        // Iterate over the rows and onvert each to RustCqlRow
        let rows_iter = rows_result
            .rows::<RustCqlRow>()
            .map_err(|e| PyRuntimeError::new_err(format!("Failed to deserialize rows: {e}")))?;

        let py_list = pyo3::types::PyList::empty(py);

        // For each row, convert to a Python dict and append to the list
        for row_res in rows_iter {
            let row = row_res
                .map_err(|e| PyRuntimeError::new_err(format!("Failed to deserialize row: {e}")))?;

            let dict = PyDict::new(py);

            for (name, opt_val) in row.columns {
                let py_val = match opt_val {
                    Some(ref cql) => cql_value_to_py(py, cql)?,
                    None => py.None(),
                };

                dict.set_item(name, py_val).map_err(|e| {
                    PyRuntimeError::new_err(format!("Failed to set dict item: {e}"))
                })?;
            }

            py_list
                .append(dict)
                .map_err(|e| PyRuntimeError::new_err(format!("Failed to append to list: {e}")))?;
        }

        Ok(py_list.into())
    }
}

#[pymodule]
pub(crate) fn session(_py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<Session>()?;
    module.add_class::<RequestResult>()?;

    Ok(())
}
