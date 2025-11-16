use std::fmt::Write;
use std::sync::Arc;

use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PyString};
use scylla::cluster::metadata::CollectionType;
use scylla::cluster::metadata::NativeType;
use scylla::frame::response::result::ColumnSpec;
use scylla::frame::response::result::ColumnType;
use scylla::serialize::row::RowSerializationContext;
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
        return Ok(RequestResult { inner: result });
    }
}

#[pyclass]
#[derive(Clone)]
pub(crate) struct PyPreparedStatement {
    pub(crate) _inner: Arc<scylla::statement::prepared::PreparedStatement>,
}

#[pyclass]
#[derive(Clone)]
pub(crate) struct PyRowSerializationContext {
    prepared_statement: Arc<scylla::statement::prepared::PreparedStatement>,
}

#[pymethods]
impl PyRowSerializationContext {
    #[staticmethod]
    fn from_prepared(prepared: PyPreparedStatement) -> Self {
        Self {
            prepared_statement: Arc::clone(&prepared._inner),
        }
    }

    fn get_context(&self) -> PyResult<PyObject> {
        Python::with_gil(|py| {
            let metadata = self.prepared_statement.get_prepared_metadata();
            let row_context = RowSerializationContext::from_prepared(metadata);
            rust_row_context_to_python(py, &row_context)
        })
    }
}

fn rust_row_context_to_python(
    py: Python<'_>,
    row_context: &RowSerializationContext,
) -> PyResult<PyObject> {
    let module = py.import("scylla.serialize.column_type")?;

    let columns_list = PyList::empty(py);
    for col_spec in row_context.columns() {
        let py_column_spec = rust_column_spec_to_python(py, col_spec)?;
        columns_list.append(py_column_spec)?;
    }

    let row_context_class = module.getattr("RowSerializationContext")?;
    let kwargs = PyDict::new(py);
    kwargs.set_item("columns", columns_list)?;

    row_context_class.call((), Some(&kwargs))?.extract()
}

fn rust_column_spec_to_python(py: Python<'_>, col_spec: &ColumnSpec) -> PyResult<PyObject> {
    let module = py.import("scylla.serialize.column_type")?;

    let table_spec_class = module.getattr("TableSpec")?;
    let table_spec_kwargs = PyDict::new(py);
    table_spec_kwargs.set_item("ks_name", col_spec.table_spec().ks_name())?;
    table_spec_kwargs.set_item("table_name", col_spec.table_spec().table_name())?;
    let py_table_spec = table_spec_class.call((), Some(&table_spec_kwargs))?;

    let py_column_type = rust_column_type_to_python(py, col_spec.typ())?;

    let column_spec_class = module.getattr("ColumnSpec")?;
    let column_spec_kwargs = PyDict::new(py);
    column_spec_kwargs.set_item("table_spec", py_table_spec)?;
    column_spec_kwargs.set_item("name", col_spec.name())?;
    column_spec_kwargs.set_item("typ", py_column_type)?;

    column_spec_class
        .call((), Some(&column_spec_kwargs))?
        .extract()
}

fn rust_column_type_to_python(py: Python<'_>, column_type: &ColumnType) -> PyResult<PyObject> {
    let module = py.import("scylla.serialize.column_type")?;

    match column_type {
        ColumnType::Native(native_type) => {
            let native_class = module.getattr("Native")?;
            let native_type_enum = module.getattr("NativeType")?;

            let enum_value = match native_type {
                NativeType::Int => native_type_enum.getattr("INT")?,
                NativeType::BigInt => native_type_enum.getattr("BIGINT")?,
                NativeType::Double => native_type_enum.getattr("DOUBLE")?,
                NativeType::Boolean => native_type_enum.getattr("BOOLEAN")?,
                NativeType::Text => native_type_enum.getattr("TEXT")?,
                _ => {
                    return Err(PyRuntimeError::new_err(format!(
                        "Unsupported native type: {:?}",
                        native_type
                    )));
                }
            };

            let kwargs = pyo3::types::PyDict::new(py);
            kwargs.set_item("type", enum_value)?;
            native_class.call((), Some(&kwargs))?.extract()
        }

        ColumnType::Collection { frozen, typ } => match typ {
            CollectionType::List(element_type) => {
                let py_element_type = rust_column_type_to_python(py, element_type)?;

                let list_class = module.getattr("List")?;

                let kwargs = pyo3::types::PyDict::new(py);
                kwargs.set_item("frozen", *frozen)?;
                kwargs.set_item("element_type", py_element_type)?;
                list_class.call((), Some(&kwargs))?.extract()
            }
            _ => {
                return Err(PyRuntimeError::new_err(format!(
                    "Unsupported collection type: {:?}",
                    typ
                )));
            }
        },

        ColumnType::UserDefinedType { frozen, definition } => {
            let udt_def_class = module.getattr("UserDefinedTypeDefinition")?;

            let field_types_list = pyo3::types::PyList::empty(py);
            for (field_name, field_type) in &definition.field_types {
                let py_field_type = rust_column_type_to_python(py, field_type)?;
                let tuple = pyo3::types::PyTuple::new(
                    py,
                    vec![
                        field_name.as_ref().into_pyobject(py).unwrap().into_any(),
                        py_field_type.into_pyobject(py).unwrap(),
                    ],
                )?;
                field_types_list.append(tuple)?;
            }

            let udt_definition =
                udt_def_class.call1((&definition.name, &definition.keyspace, field_types_list))?;

            let udt_class = module.getattr("UserDefinedType")?;
            let kwargs = pyo3::types::PyDict::new(py);
            kwargs.set_item("frozen", *frozen)?;
            kwargs.set_item("definition", udt_definition)?;
            udt_class.call((), Some(&kwargs))?.extract()
        }
        _ => Err(PyRuntimeError::new_err(format!(
            "Unsupported column type: {:?}",
            column_type
        ))),
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
    module.add_class::<PyPreparedStatement>()?;
    module.add_class::<PyRowSerializationContext>()?;
    Ok(())
}
