use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;
use scylla::frame::response::result::{CollectionType, ColumnType, NativeType};
use scylla::response::query_result::QueryResult;
use scylla::statement::prepared;

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

/// Specification of a partition key index in prepared statement metadata.
#[pyclass(skip_from_py_object, frozen)]
#[derive(Clone)]
pub(crate) struct PyPartitionKeyIndex {
    /// The index of the partition key.
    #[pyo3(get)]
    index: u16,
    /// The sequence number of the partition key, used for multi-column partition keys.
    #[pyo3(get)]
    sequence_number: u16,
}

/// Metadata for a prepared statement, including column specifications and partition key indexes.
#[pyclass(skip_from_py_object, frozen)]
#[derive(Clone)]
pub(crate) struct PyPreparedMetadata {
    /// The specifications of the columns in the prepared statement.
    #[pyo3(get)]
    columns: Vec<PyColumnSpec>,
    /// The indexes of the partition keys in the prepared statement.
    #[pyo3(get)]
    partition_key_indexes: Vec<PyPartitionKeyIndex>,
}

/// Metadata for a query result, including column specifications and column count.
#[pyclass(skip_from_py_object, frozen)]
#[derive(Clone)]
pub(crate) struct PyResultMetadata {
    /// The number of columns in the query result.
    #[pyo3(get)]
    column_count: usize,
    /// The specifications of the columns in the query result.
    #[pyo3(get)]
    columns: Vec<PyColumnSpec>,
}

/* Conversion helpers */

/// Converts a Scylla NativeType to its CQL string representation.
fn native_type_to_cql(typ: &NativeType) -> &'static str {
    match typ {
        NativeType::Ascii => "ascii",
        NativeType::Boolean => "boolean",
        NativeType::Blob => "blob",
        NativeType::Counter => "counter",
        NativeType::Date => "date",
        NativeType::Decimal => "decimal",
        NativeType::Double => "double",
        NativeType::Duration => "duration",
        NativeType::Float => "float",
        NativeType::Int => "int",
        NativeType::BigInt => "bigint",
        NativeType::Text => "text",
        NativeType::Timestamp => "timestamp",
        NativeType::Inet => "inet",
        NativeType::SmallInt => "smallint",
        NativeType::TinyInt => "tinyint",
        NativeType::Time => "time",
        NativeType::Timeuuid => "timeuuid",
        NativeType::Uuid => "uuid",
        NativeType::Varint => "varint",
        _ => "unknown",
    }
}

/// Converts a Scylla ColumnType to its CQL string representation, handling native types, collections, UDTs, tuples, and vectors.
fn column_type_to_cql(typ: &ColumnType<'_>) -> String {
    match typ {
        ColumnType::Native(native) => native_type_to_cql(native).to_owned(),

        ColumnType::Collection { frozen, typ } => {
            let inner = match typ {
                CollectionType::List(elem) => {
                    format!("list<{}>", column_type_to_cql(elem))
                }
                CollectionType::Set(elem) => {
                    format!("set<{}>", column_type_to_cql(elem))
                }
                CollectionType::Map(key, value) => {
                    format!(
                        "map<{}, {}>",
                        column_type_to_cql(key),
                        column_type_to_cql(value)
                    )
                }
                _ => "unknown".to_owned(),
            };

            if *frozen {
                format!("frozen<{}>", inner)
            } else {
                inner
            }
        }

        ColumnType::Vector { typ, dimensions } => {
            format!("vector<{}, {}>", column_type_to_cql(typ), dimensions)
        }

        ColumnType::UserDefinedType { frozen, definition } => {
            let base = format!("{}.{}", definition.keyspace, definition.name);

            if *frozen {
                format!("frozen<{}>", base)
            } else {
                base
            }
        }

        ColumnType::Tuple(types) => {
            let inner = types
                .iter()
                .map(column_type_to_cql)
                .collect::<Vec<_>>()
                .join(", ");
            format!("tuple<{}>", inner)
        }

        _ => "unknown".to_owned(),
    }
}

/// Converts a Scylla ColumnSpec to a PyColumnSpec.
fn column_spec_to_py(spec: &scylla::frame::response::result::ColumnSpec<'_>) -> PyColumnSpec {
    let table_spec = spec.table_spec();
    let table_name = table_spec.table_name();
    let keyspace_name = table_spec.ks_name();

    PyColumnSpec {
        name: spec.name().to_owned(),
        table_name: (!table_name.is_empty()).then(|| table_name.to_owned()),
        keyspace_name: (!keyspace_name.is_empty()).then(|| keyspace_name.to_owned()),
        cql_type: column_type_to_cql(spec.typ()),
    }
}

/// Converts a Scylla PartitionKeyIndex to a PyPartitionKeyIndex.
fn partition_key_to_py(
    pk: &scylla::frame::response::result::PartitionKeyIndex,
) -> PyPartitionKeyIndex {
    PyPartitionKeyIndex {
        index: pk.index,
        sequence_number: pk.sequence,
    }
}

/// Creates a PyPreparedMetadata from a Scylla PreparedStatement.
pub(crate) fn prepared_metadata_from_prepared(
    prepared: &prepared::PreparedStatement,
) -> PyPreparedMetadata {
    let columns = prepared
        .get_variable_col_specs()
        .iter()
        .map(column_spec_to_py)
        .collect();

    let partition_key_indexes = prepared
        .get_variable_pk_indexes()
        .iter()
        .map(partition_key_to_py)
        .collect();

    PyPreparedMetadata {
        columns,
        partition_key_indexes,
    }
}

/// Creates a PyResultMetadata from a Scylla PreparedStatement by extracting the current result set column specifications.
pub(crate) fn result_metadata_from_prepared(
    prepared: &prepared::PreparedStatement,
) -> PyResultMetadata {
    let guard = prepared.get_current_result_set_col_specs();

    let columns: Vec<PyColumnSpec> = guard
        .get()
        .iter()
        .map(|spec| column_spec_to_py(spec))
        .collect();

    PyResultMetadata {
        column_count: columns.len(),
        columns,
    }
}

/// Creates a PyResultMetadata from a Scylla QueryResult, if it contains rows metadata.
pub(crate) fn result_metadata_from_query_result(
    query_result: &QueryResult,
) -> PyResult<PyResultMetadata> {
    if !query_result.is_rows() {
        return Ok(PyResultMetadata {
            column_count: 0,
            columns: Vec::new(),
        });
    }

    let raw_rows_with_metadata = query_result
        .deserialized_metadata_and_rows()
        .ok_or_else(|| PyErr::new::<PyRuntimeError, _>("Expected rows metadata, but got none"))?;

    let columns: Vec<PyColumnSpec> = raw_rows_with_metadata
        .metadata()
        .col_specs()
        .iter()
        .map(|spec| column_spec_to_py(spec))
        .collect();

    Ok(PyResultMetadata {
        column_count: columns.len(),
        columns,
    })
}

pub(crate) fn query_metadata(_py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<PyColumnSpec>()?;
    module.add_class::<PyPartitionKeyIndex>()?;
    module.add_class::<PyPreparedMetadata>()?;
    module.add_class::<PyResultMetadata>()?;

    Ok(())
}
