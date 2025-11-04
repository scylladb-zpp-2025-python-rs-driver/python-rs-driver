use std::fmt::Write;
use std::sync::Arc;

use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::PyString;
use scylla::cluster::metadata::CollectionType;
use scylla::cluster::metadata::NativeType;
use scylla::frame::response::result::ColumnSpec;
use scylla::frame::response::result::ColumnType;
use scylla::serialize::row::RowSerializationContext;
use scylla::value::Row;
use scylla_cql::frame::request::query::PagingState;
use scylla_cql::frame::response::result::UserDefinedType;
use scylla_cql::serialize::row::SerializedValues;

use crate::RUNTIME;

#[pyclass]
pub(crate) struct Session {
    pub(crate) _inner: Arc<scylla::client::session::Session>,
}

#[pyclass]
#[derive(Clone)]
pub(crate) struct PyPreparedStatement {
    pub(crate) _inner: Arc<scylla::statement::prepared::PreparedStatement>,
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
        return Ok(PyPreparedStatement {
            _inner: (Arc::new(result)),
        });
    }

    async fn _execute_raw_bytes(
        &self,
        prepared: PyPreparedStatement,
        serialized_values: Vec<u8>,
        element_count: u16,
    ) -> PyResult<RequestResult> {
        let prep = Arc::clone(&prepared._inner);
        let session_clone = Arc::clone(&self._inner);

        let ser = SerializedValues {
            serialized_values: serialized_values,
            element_count: element_count,
        };

        let (result, _) = RUNTIME
            .spawn(async move {
                session_clone
                    .execute(&prep, &ser, None, PagingState::start())
                    .await
                    .map_err(|e| PyRuntimeError::new_err(format!("Failed to execute query: {}", e)))
            })
            .await
            .expect("Driver should not panic")?;
        return Ok(RequestResult { inner: result });
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

#[pyclass]
#[derive(Clone)]
pub(crate) struct PyRowSerializationContext {
    prepared_statement: Arc<scylla::statement::prepared::PreparedStatement>,
}

#[pymethods]
impl PyRowSerializationContext {
    /// Create a new PyRowSerializationContext from a PyPreparedStatement
    #[staticmethod]
    fn from_prepared(prepared: PyPreparedStatement) -> Self {
        Self {
            prepared_statement: Arc::clone(&prepared._inner),
        }
    }

    /// Get column specifications as a list of dictionaries
    /// This provides Python access to column metadata
    fn columns(&self) -> PyResult<Vec<PyObject>> {
        Python::with_gil(|py| {
            let metadata = self.prepared_statement.get_prepared_metadata();
            let context = RowSerializationContext::from_prepared(metadata);

            let mut columns = Vec::new();
            for col_spec in context.columns() {
                let col_dict = pyo3::types::PyDict::new(py);

                // Add column name
                col_dict.set_item("name", col_spec.name())?;

                // Extract structured type information
                let type_info = extract_type_info(py, col_spec.typ())?;
                col_dict.set_item("type_struct", type_info)?;

                columns.push(col_dict.into());
            }

            Ok(columns)
        })
    }
}

fn extract_type_info(py: Python, column_type: &ColumnType) -> PyResult<PyObject> {
    let type_dict = pyo3::types::PyDict::new(py);

    match column_type {
        // POC: Handle Native types with full structure
        ColumnType::Native(native_type) => {
            type_dict.set_item("kind", "native")?;

            match native_type {
                NativeType::Int => {
                    type_dict.set_item("name", "int")?;
                    type_dict.set_item("size", 4)?; // 4 bytes
                    type_dict.set_item("signed", true)?;
                    type_dict.set_item("wire_type", "int32")?;
                }
                NativeType::Text => {
                    type_dict.set_item("name", "text")?;
                    type_dict.set_item("wire_type", "bytes")?;
                    type_dict.set_item("encoding", "utf8")?;
                }
                NativeType::BigInt => {
                    type_dict.set_item("name", "bigint")?;
                    type_dict.set_item("size", 8)?; // 8 bytes
                    type_dict.set_item("signed", true)?;
                    type_dict.set_item("wire_type", "int64")?;
                }
                NativeType::Double => {
                    type_dict.set_item("name", "double")?;
                    type_dict.set_item("size", 8)?; // 8 bytes
                    type_dict.set_item("wire_type", "double")?;
                }
                NativeType::Boolean => {
                    type_dict.set_item("name", "boolean")?;
                    type_dict.set_item("size", 1)?; // 1 byte
                    type_dict.set_item("wire_type", "boolean")?;
                }
                // Easily extendable - just add more native types here
                _ => {
                    type_dict.set_item("name", "unknown_native")?;
                    type_dict.set_item("debug", format!("{:?}", native_type))?;
                }
            }
        }

        // Handle Collection types with recursive structure
        ColumnType::Collection { frozen, typ } => {
            type_dict.set_item("kind", "collection")?;
            type_dict.set_item("frozen", *frozen)?;

            match typ {
                CollectionType::List(element_type) => {
                    type_dict.set_item("name", "list")?;
                    // Recursive call for element type
                    let element_info = extract_type_info(py, element_type)?;
                    type_dict.set_item("element_type", element_info)?;
                }
                CollectionType::Set(element_type) => {
                    type_dict.set_item("name", "set")?;
                    // Recursive call for element type
                    let element_info = extract_type_info(py, element_type)?;
                    type_dict.set_item("element_type", element_info)?;
                }
                CollectionType::Map(key_type, value_type) => {
                    type_dict.set_item("name", "map")?;
                    // Recursive calls for key and value types
                    let key_info = extract_type_info(py, key_type)?;
                    let value_info = extract_type_info(py, value_type)?;
                    type_dict.set_item("key_type", key_info)?;
                    type_dict.set_item("value_type", value_info)?;
                }
                _ => {
                    type_dict.set_item("name", "unknown_collection")?;
                    type_dict.set_item("debug", format!("{:?}", typ))?;
                }
            }
        }

        // Handle UDT types
        ColumnType::UserDefinedType { frozen, definition } => {
            type_dict.set_item("kind", "user_defined")?; // ✅ Correct kind for Python
            type_dict.set_item("frozen", *frozen)?; // ✅ Add frozen like Collections
            type_dict.set_item("name", &definition.name)?; // ✅ UDT name
            type_dict.set_item("keyspace", &definition.keyspace)?; // ✅ Add keyspace info

            // ✅ Create structured fields array (following Collection pattern)
            let fields = pyo3::types::PyList::empty(py); // ✅ Full path like existing code

            // ✅ Iterate over field_types: Vec<(field_name, field_type)>
            for (field_name, field_type) in &definition.field_types {
                let field_dict = pyo3::types::PyDict::new(py); // ✅ Full path like existing code
                field_dict.set_item("name", field_name.as_ref())?;

                // ✅ Recursive call for field type - same pattern as Collections!
                let field_type_info = extract_type_info(py, field_type)?;
                field_dict.set_item("type", field_type_info)?;

                fields.append(field_dict)?;
            }

            type_dict.set_item("fields", fields)?;
        }

        // Handle Tuple types
        ColumnType::Tuple(element_types) => {
            type_dict.set_item("kind", "tuple")?;

            // Create elements array with recursive type extraction
            let elements_list = pyo3::types::PyList::empty(py);
            for element_type in element_types {
                let element_info = extract_type_info(py, element_type)?;
                elements_list.append(element_info)?;
            }
            type_dict.set_item("element_types", elements_list)?;
        }

        // Default case for types not yet implemented
        _ => {
            type_dict.set_item("kind", "unknown")?;
            type_dict.set_item("debug", format!("{:?}", column_type))?;
        }
    }

    Ok(type_dict.into())
}

#[pymodule]
pub(crate) fn session(_py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<Session>()?;
    module.add_class::<RequestResult>()?;
    module.add_class::<PyPreparedStatement>()?;
    module.add_class::<PyRowSerializationContext>()?;

    Ok(())
}
