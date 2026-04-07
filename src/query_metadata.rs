use pyo3::prelude::*;

/// Specification of a column in a result set, used for both prepared statement metadata and query result metadata.
#[pyclass(skip_from_py_object, frozen)]
#[derive(Clone)]
pub(crate) struct PyColumnSpec {
    /// The name of the column.
    #[pyo3(get)]
    name: String,
    /// The name of the table containing the column.
    /// This is `None` for columns that are not associated with a specific table (e.g., result of an aggregation).
    #[pyo3(get)]
    table_name: Option<String>,
    /// The name of the keyspace containing the column.
    /// This is `None` for columns that are not associated with a specific keyspace (e.g., result of an aggregation).
    #[pyo3(get)]
    keyspace_name: Option<String>,
    /// The CQL type of the column.
    #[pyo3(get)]
    cql_type: String,
}

/* Conversion helpers */

/// Converts a Scylla ColumnSpec to a PyColumnSpec.
#[allow(dead_code)]
fn column_spec_to_py(spec: &scylla::frame::response::result::ColumnSpec<'_>) -> PyColumnSpec {
    let table_spec = spec.table_spec();
    let table_name = table_spec.table_name();
    let keyspace_name = table_spec.ks_name();

    PyColumnSpec {
        name: spec.name().to_owned(),
        table_name: (!table_name.is_empty()).then(|| table_name.to_owned()),
        keyspace_name: (!keyspace_name.is_empty()).then(|| keyspace_name.to_owned()),
        // Temporarily use the debug representation of the CQL type.
        cql_type: format!("{:?}", spec.typ()),
    }
}

pub(crate) fn query_metadata(_py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<PyColumnSpec>()?;

    Ok(())
}
