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

pub(crate) fn query_metadata(_py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<PyColumnSpec>()?;
    module.add_class::<PyPartitionKeyIndex>()?;

    Ok(())
}
