use std::sync::Arc;

use pyo3::{
    prelude::*,
    types::{PyDict, PyList, PyString},
};
use scylla::cluster;
use scylla_cql::frame::response::result::UserDefinedType as RustUserDefinedType;

#[pyclass(frozen, eq, eq_int)]
#[derive(Clone, PartialEq)]
pub(crate) enum ColumnKind {
    Regular,
    Static,
    Clustering,
    PartitionKey,
}

impl ColumnKind {
    fn from_rust(kind: &cluster::metadata::ColumnKind) -> Self {
        match kind {
            cluster::metadata::ColumnKind::Regular => ColumnKind::Regular,
            cluster::metadata::ColumnKind::Static => ColumnKind::Static,
            cluster::metadata::ColumnKind::Clustering => ColumnKind::Clustering,
            cluster::metadata::ColumnKind::PartitionKey => ColumnKind::PartitionKey,
            _ => ColumnKind::Regular,
        }
    }
}

#[pyclass(frozen)]
#[derive(Clone)]
pub(crate) struct Column {
    _inner: cluster::metadata::Column,
}

#[pymethods]
impl Column {
    #[getter]
    fn typ(&self) -> String {
        format!("{:?}", self._inner.typ)
    }

    #[getter]
    fn kind(&self) -> ColumnKind {
        ColumnKind::from_rust(&self._inner.kind)
    }

    fn __repr__(&self) -> String {
        format!(
            "Column(typ='{:?}', kind={:?})",
            self._inner.typ, self._inner.kind
        )
    }
}

#[pyclass(frozen)]
#[derive(Clone)]
pub(crate) struct Table {
    _inner: cluster::metadata::Table,
}

#[pymethods]
impl Table {
    #[getter]
    fn columns<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        columns_to_pydict(py, &self._inner.columns)
    }

    #[getter]
    fn partition_key<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        PyList::new(py, &self._inner.partition_key)
    }

    #[getter]
    fn clustering_key<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        PyList::new(py, &self._inner.clustering_key)
    }

    #[getter]
    fn partitioner<'py>(&self, py: Python<'py>) -> Option<Bound<'py, PyString>> {
        self._inner
            .partitioner
            .as_ref()
            .map(|p| PyString::new(py, p))
    }

    fn __repr__(&self) -> String {
        format!(
            "Table(columns={}, partition_key={:?}, clustering_key={:?})",
            self._inner.columns.len(),
            self._inner.partition_key,
            self._inner.clustering_key,
        )
    }
}

#[pyclass(frozen)]
#[derive(Clone)]
pub(crate) struct MaterializedView {
    _inner: cluster::metadata::MaterializedView,
}

#[pymethods]
impl MaterializedView {
    #[getter]
    fn base_table_name<'py>(&self, py: Python<'py>) -> Bound<'py, PyString> {
        PyString::new(py, &self._inner.base_table_name)
    }

    #[getter]
    fn view_columns<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        columns_to_pydict(py, &self._inner.view_metadata.columns)
    }

    #[getter]
    fn view_partition_key<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        PyList::new(py, &self._inner.view_metadata.partition_key)
    }

    #[getter]
    fn view_clustering_key<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        PyList::new(py, &self._inner.view_metadata.clustering_key)
    }

    #[getter]
    fn view_partitioner<'py>(&self, py: Python<'py>) -> Option<Bound<'py, PyString>> {
        self._inner
            .view_metadata
            .partitioner
            .as_ref()
            .map(|p| PyString::new(py, p))
    }

    fn __repr__(&self) -> String {
        format!(
            "MaterializedView(base_table='{}', columns={})",
            self._inner.base_table_name,
            self._inner.view_metadata.columns.len(),
        )
    }
}

#[pyclass(frozen)]
#[derive(Clone)]
pub(crate) struct UserDefinedType {
    _inner: Arc<RustUserDefinedType<'static>>,
}

#[pymethods]
impl UserDefinedType {
    #[getter]
    fn name<'py>(&self, py: Python<'py>) -> Bound<'py, PyString> {
        PyString::new(py, &self._inner.name)
    }

    #[getter]
    fn keyspace<'py>(&self, py: Python<'py>) -> Bound<'py, PyString> {
        PyString::new(py, &self._inner.keyspace)
    }

    fn __repr__(&self) -> String {
        let fields: Vec<&str> = self
            ._inner
            .field_types
            .iter()
            .map(|(name, _)| name.as_ref())
            .collect();
        format!(
            "UserDefinedType(keyspace='{}', name='{}', fields={:?})",
            self._inner.keyspace, self._inner.name, fields
        )
    }
}

#[pyclass]
pub(crate) struct Keyspace {
    pub(crate) _inner: cluster::metadata::Keyspace,
}

#[pymethods]
impl Keyspace {
    #[getter]
    fn strategy_name(&self) -> &str {
        match &self._inner.strategy {
            cluster::metadata::Strategy::SimpleStrategy { .. } => "SimpleStrategy",
            cluster::metadata::Strategy::NetworkTopologyStrategy { .. } => {
                "NetworkTopologyStrategy"
            }
            cluster::metadata::Strategy::LocalStrategy => "LocalStrategy",
            cluster::metadata::Strategy::Other { .. } => "Other",
            _ => "Unknown",
        }
    }

    #[getter]
    fn strategy_replication_factor(&self) -> Option<usize> {
        match self._inner.strategy {
            cluster::metadata::Strategy::SimpleStrategy {
                replication_factor, ..
            } => Some(replication_factor),
            _ => None,
        }
    }

    #[getter]
    fn strategy_datacenter_repfactors<'py>(
        &self,
        py: Python<'py>,
    ) -> PyResult<Option<Bound<'py, PyDict>>> {
        match &self._inner.strategy {
            cluster::metadata::Strategy::NetworkTopologyStrategy {
                datacenter_repfactors,
            } => {
                let dict = PyDict::new(py);
                for (dc, factor) in datacenter_repfactors {
                    dict.set_item(dc, factor)?;
                }
                Ok(Some(dict))
            }
            _ => Ok(None),
        }
    }

    #[getter]
    fn strategy_other_name<'py>(&self, py: Python<'py>) -> Option<Bound<'py, PyString>> {
        match &self._inner.strategy {
            cluster::metadata::Strategy::Other { name, .. } => Some(PyString::new(py, name)),
            _ => None,
        }
    }

    #[getter]
    fn strategy_other_data<'py>(&self, py: Python<'py>) -> PyResult<Option<Bound<'py, PyDict>>> {
        match &self._inner.strategy {
            cluster::metadata::Strategy::Other { data, .. } => {
                let dict = PyDict::new(py);
                for (key, val) in data {
                    dict.set_item(key, val)?;
                }
                Ok(Some(dict))
            }
            _ => Ok(None),
        }
    }

    #[getter]
    fn tables<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        let dict = PyDict::new(py);
        for (name, table) in &self._inner.tables {
            let py_table = Py::new(
                py,
                Table {
                    _inner: table.clone(),
                },
            )?;
            dict.set_item(name, py_table)?;
        }
        Ok(dict)
    }

    #[getter]
    fn views<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        let dict = PyDict::new(py);
        for (name, view) in &self._inner.views {
            let py_view = Py::new(
                py,
                MaterializedView {
                    _inner: view.clone(),
                },
            )?;
            dict.set_item(name, py_view)?;
        }
        Ok(dict)
    }

    #[getter]
    fn user_defined_types<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        let dict = PyDict::new(py);
        for (name, udt) in &self._inner.user_defined_types {
            let py_udt = Py::new(
                py,
                UserDefinedType {
                    _inner: Arc::clone(udt),
                },
            )?;
            dict.set_item(name, py_udt)?;
        }
        Ok(dict)
    }

    fn __repr__(&self) -> String {
        let strategy_repr = strategy_repr_string(&self._inner.strategy);
        format!(
            "Keyspace(strategy={}, tables={}, views={}, udts={})",
            strategy_repr,
            self._inner.tables.len(),
            self._inner.views.len(),
            self._inner.user_defined_types.len(),
        )
    }
}

fn strategy_repr_string(strategy: &cluster::metadata::Strategy) -> String {
    match strategy {
        cluster::metadata::Strategy::SimpleStrategy {
            replication_factor, ..
        } => format!("Strategy(SimpleStrategy, replication_factor={replication_factor})"),
        cluster::metadata::Strategy::NetworkTopologyStrategy {
            datacenter_repfactors,
        } => {
            format!(
                "Strategy(NetworkTopologyStrategy, datacenter_repfactors={datacenter_repfactors:?})"
            )
        }
        cluster::metadata::Strategy::LocalStrategy => "Strategy(LocalStrategy)".to_string(),
        cluster::metadata::Strategy::Other { name, data } => {
            format!("Strategy(Other, name='{name}', data={data:?})")
        }
        _ => "Strategy(Unknown)".to_string(),
    }
}

fn columns_to_pydict<'py>(
    py: Python<'py>,
    columns: &std::collections::HashMap<String, cluster::metadata::Column>,
) -> PyResult<Bound<'py, PyDict>> {
    let dict = PyDict::new(py);
    for (name, col) in columns {
        let py_col = Py::new(
            py,
            Column {
                _inner: col.clone(),
            },
        )?;
        dict.set_item(name, py_col)?;
    }
    Ok(dict)
}

#[pymodule]
pub(crate) fn metadata(_py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<Keyspace>()?;
    module.add_class::<Table>()?;
    module.add_class::<Column>()?;
    module.add_class::<ColumnKind>()?;
    module.add_class::<MaterializedView>()?;
    module.add_class::<UserDefinedType>()?;
    Ok(())
}
