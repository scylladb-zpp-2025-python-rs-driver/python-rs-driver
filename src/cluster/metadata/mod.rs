use pyo3::{
    prelude::*,
    types::{IntoPyDict, PyDict, PyInt, PyList, PyString},
};
use scylla::cluster;

#[pyclass(eq, eq_int, frozen, skip_from_py_object)]
#[derive(Clone, Copy, PartialEq)]
pub(crate) enum StrategyKind {
    Simple,
    NetworkTopology,
    Local,
    Other,
}

#[pyclass(frozen, from_py_object)]
#[derive(Clone)]
pub(crate) struct Strategy {
    pub(crate) _inner: cluster::metadata::Strategy,
}

impl Strategy {
    pub(crate) fn from_rust(s: cluster::metadata::Strategy) -> Self {
        Strategy { _inner: s }
    }
}

#[pymethods]
impl Strategy {
    #[getter]
    fn kind(&self) -> StrategyKind {
        match &self._inner {
            cluster::metadata::Strategy::SimpleStrategy { .. } => StrategyKind::Simple,
            cluster::metadata::Strategy::NetworkTopologyStrategy { .. } => {
                StrategyKind::NetworkTopology
            }
            cluster::metadata::Strategy::LocalStrategy => StrategyKind::Local,
            _ => StrategyKind::Other,
        }
    }

    #[getter]
    fn replication_factor(&self) -> Option<usize> {
        match self._inner {
            cluster::metadata::Strategy::SimpleStrategy {
                replication_factor, ..
            } => Some(replication_factor),
            _ => None,
        }
    }

    #[getter]
    fn datacenter_repfactors<'py>(&self, py: Python<'py>) -> PyResult<Option<Bound<'py, PyDict>>> {
        match &self._inner {
            cluster::metadata::Strategy::NetworkTopologyStrategy {
                datacenter_repfactors,
            } => Some(
                datacenter_repfactors
                    .iter()
                    .map(|(dc, factor)| (PyString::new(py, dc), PyInt::new(py, factor)))
                    .into_py_dict(py),
            )
            .transpose(),
            _ => Ok(None),
        }
    }

    #[getter]
    fn other_name<'py>(&self, py: Python<'py>) -> Option<Bound<'py, PyString>> {
        match &self._inner {
            cluster::metadata::Strategy::Other { name, .. } => Some(PyString::new(py, name)),
            _ => None,
        }
    }

    #[getter]
    fn other_data<'py>(&self, py: Python<'py>) -> PyResult<Option<Bound<'py, PyDict>>> {
        match &self._inner {
            cluster::metadata::Strategy::Other { data, .. } => Some(
                data.iter()
                    .map(|(key, val)| (PyString::new(py, key), PyString::new(py, val)))
                    .into_py_dict(py),
            )
            .transpose(),
            _ => Ok(None),
        }
    }

    fn __repr__(&self) -> String {
        format!("{:?}", self._inner)
    }
}

#[pyclass(frozen, eq, eq_int, skip_from_py_object)]
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

#[pyclass(frozen, skip_from_py_object)]
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

#[pyclass(frozen, skip_from_py_object)]
#[derive(Clone)]
pub(crate) struct Table {
    _inner: cluster::metadata::Table,
}

#[pymethods]
impl Table {
    #[getter]
    fn columns<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        self._inner
            .columns
            .iter()
            .map(|(name, col)| {
                (
                    PyString::new(py, name),
                    Column {
                        _inner: col.clone(),
                    },
                )
            })
            .into_py_dict(py)
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

#[pyclass(frozen, skip_from_py_object)]
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
        self._inner
            .view_metadata
            .columns
            .iter()
            .map(|(name, col)| {
                (
                    PyString::new(py, name),
                    Column {
                        _inner: col.clone(),
                    },
                )
            })
            .into_py_dict(py)
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
pub(crate) struct Keyspace {
    pub(crate) _inner: cluster::metadata::Keyspace,
}

#[pymethods]
impl Keyspace {
    #[getter]
    fn strategy(&self) -> Strategy {
        Strategy::from_rust(self._inner.strategy.clone())
    }

    #[getter]
    fn tables<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        self._inner
            .tables
            .iter()
            .map(|(name, table)| {
                (
                    PyString::new(py, name),
                    Table {
                        _inner: table.clone(),
                    },
                )
            })
            .into_py_dict(py)
    }

    #[getter]
    fn views<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        self._inner
            .views
            .iter()
            .map(|(name, view)| {
                (
                    PyString::new(py, name),
                    MaterializedView {
                        _inner: view.clone(),
                    },
                )
            })
            .into_py_dict(py)
    }

    fn __repr__(&self) -> String {
        format!(
            "Keyspace(strategy={:?}, tables={}, views={}, udts={})",
            self._inner.strategy,
            self._inner.tables.len(),
            self._inner.views.len(),
            self._inner.user_defined_types.len(),
        )
    }
}

#[pymodule]
pub(crate) fn metadata(_py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<Keyspace>()?;
    module.add_class::<Table>()?;
    module.add_class::<Column>()?;
    module.add_class::<ColumnKind>()?;
    module.add_class::<MaterializedView>()?;
    module.add_class::<Strategy>()?;
    module.add_class::<StrategyKind>()?;
    Ok(())
}
