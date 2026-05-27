use pyo3::prelude::*;

use crate::cluster::metadata::column_type::{PyCqlColumnType, extract_column_type};
use crate::errors::DriverQueryMetadataError;

/// Specification of a column in a result set, used for both prepared statement metadata and query result metadata.
#[pyclass(name = "ColumnSpec", skip_from_py_object, frozen, get_all)]
#[derive(Clone)]
pub(crate) struct PyColumnSpec {
    /// The name of the column.
    name: String,
    /// The name of the table containing the column.
    table_name: String,
    /// The name of the keyspace containing the column.
    keyspace_name: String,
    /// The CQL type of the column.
    cql_type: Py<PyCqlColumnType>,
}

// Convert from Scylla's `ColumnSpec` to our `PyColumnSpec`, extracting the CQL type information in the process.
impl<'a> TryFrom<(Python<'a>, &scylla::frame::response::result::ColumnSpec<'a>)> for PyColumnSpec {
    type Error = DriverQueryMetadataError;

    fn try_from(
        (py, spec): (Python<'a>, &scylla::frame::response::result::ColumnSpec<'a>),
    ) -> Result<Self, Self::Error> {
        let table_spec = spec.table_spec();
        let table_name = table_spec.table_name();
        let keyspace_name = table_spec.ks_name();

        let cql_type = extract_column_type(py, spec.typ())
            .map_err(DriverQueryMetadataError::column_type_extraction_failed)?;

        Ok(PyColumnSpec {
            name: spec.name().to_owned(),
            table_name: table_name.to_owned(),
            keyspace_name: keyspace_name.to_owned(),
            cql_type,
        })
    }
}

/// Specification of a partition key index in prepared statement metadata.
#[pyclass(name = "PartitionKeyIndex", skip_from_py_object, frozen, get_all)]
#[derive(Clone)]
pub(crate) struct PyPartitionKeyIndex {
    /// The index of the partition key.
    index: u16,
    /// The sequence number of the partition key, used for multi-column partition keys.
    sequence_number: u16,
}

// Convert from Scylla's `PartitionKeyIndex` to our `PyPartitionKeyIndex`.
impl From<&scylla::frame::response::result::PartitionKeyIndex> for PyPartitionKeyIndex {
    fn from(pk: &scylla::frame::response::result::PartitionKeyIndex) -> Self {
        PyPartitionKeyIndex {
            index: pk.index,
            sequence_number: pk.sequence,
        }
    }
}

/// Metadata for a prepared statement, including column specifications and partition key indexes.
#[pyclass(name = "PreparedMetadata", skip_from_py_object, frozen, get_all)]
#[derive(Clone)]
pub(crate) struct PyPreparedMetadata {
    /// The specifications of the columns in the prepared statement.
    columns: Vec<PyColumnSpec>,
    /// The indexes of the partition keys in the prepared statement.
    partition_key_indexes: Vec<PyPartitionKeyIndex>,
}

// Convert from Scylla's `PreparedStatement` to our `PyPreparedMetadata`.
impl<'a> TryFrom<(Python<'a>, &scylla::statement::prepared::PreparedStatement)>
    for PyPreparedMetadata
{
    type Error = DriverQueryMetadataError;

    fn try_from(
        (py, prepared): (Python<'a>, &scylla::statement::prepared::PreparedStatement),
    ) -> Result<Self, Self::Error> {
        // Uses PyColumnSpec::try_from via the tuple syntax
        let columns = prepared
            .get_variable_col_specs()
            .iter()
            .map(|spec| PyColumnSpec::try_from((py, spec)))
            .collect::<Result<Vec<PyColumnSpec>, DriverQueryMetadataError>>()?;

        // Uses PyPartitionKeyIndex::from
        let partition_key_indexes = prepared
            .get_variable_pk_indexes()
            .iter()
            .map(PyPartitionKeyIndex::from)
            .collect();

        Ok(PyPreparedMetadata {
            columns,
            partition_key_indexes,
        })
    }
}

/// Metadata for a query result, including column specifications and column count.
#[pyclass(name = "ResultMetadata", skip_from_py_object, frozen, get_all)]
#[derive(Clone)]
pub(crate) struct PyResultMetadata {
    /// The number of columns in the query result.
    column_count: usize,
    /// The specifications of the columns in the query result.
    columns: Vec<PyColumnSpec>,
}

// Convert from Scylla's `PreparedStatement` to our `PyResultMetadata`.
impl<'a> TryFrom<(Python<'a>, &scylla::statement::prepared::PreparedStatement)>
    for PyResultMetadata
{
    type Error = DriverQueryMetadataError;

    fn try_from(
        (py, prepared): (Python<'a>, &scylla::statement::prepared::PreparedStatement),
    ) -> Result<Self, Self::Error> {
        let guard = prepared.get_current_result_set_col_specs();

        let columns = guard
            .get()
            .iter()
            .map(|spec| PyColumnSpec::try_from((py, spec)))
            .collect::<Result<Vec<PyColumnSpec>, DriverQueryMetadataError>>()?;

        Ok(PyResultMetadata {
            column_count: columns.len(),
            columns,
        })
    }
}

// Convert from Scylla's `QueryResult` to our `PyResultMetadata`.
impl<'a> TryFrom<(Python<'a>, &scylla::response::query_result::QueryResult)> for PyResultMetadata {
    type Error = DriverQueryMetadataError;

    fn try_from(
        (py, query_result): (Python<'a>, &scylla::response::query_result::QueryResult),
    ) -> Result<Self, Self::Error> {
        if !query_result.is_rows() {
            return Ok(PyResultMetadata {
                column_count: 0,
                columns: Vec::new(),
            });
        }

        let raw_rows_with_metadata = query_result
            .deserialized_metadata_and_rows()
            .ok_or_else(DriverQueryMetadataError::missing_rows_metadata)?;

        let columns = raw_rows_with_metadata
            .metadata()
            .col_specs()
            .iter()
            .map(|spec| PyColumnSpec::try_from((py, spec)))
            .collect::<Result<Vec<PyColumnSpec>, DriverQueryMetadataError>>()?;

        Ok(PyResultMetadata {
            column_count: columns.len(),
            columns,
        })
    }
}

pub(crate) fn query_metadata(_py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<PyColumnSpec>()?;
    module.add_class::<PyPartitionKeyIndex>()?;
    module.add_class::<PyPreparedMetadata>()?;
    module.add_class::<PyResultMetadata>()?;

    Ok(())
}
