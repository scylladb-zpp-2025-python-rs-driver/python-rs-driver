use std::sync::OnceLock;

use pyo3::{
    IntoPyObjectExt,
    prelude::*,
    sync::OnceLockExt,
    types::{IntoPyDict, PyDict, PyMappingProxy, PyString},
};
use scylla::cluster::metadata::{Column, ColumnKind, Keyspace, Strategy, Table};

use crate::cache::Cache;

#[pyclass(name = "StrategyKind", eq, eq_int, frozen, skip_from_py_object)]
#[derive(Clone, Copy, PartialEq)]
pub(crate) enum PyStrategyKind {
    Simple,
    NetworkTopology,
    Local,
    Other,
}

#[pyclass(name = "Strategy", frozen, skip_from_py_object)]
#[derive(Clone)]
pub(crate) struct PyStrategy {
    pub(crate) _inner: Strategy,
}

impl From<Strategy> for PyStrategy {
    fn from(inner: Strategy) -> Self {
        PyStrategy { _inner: inner }
    }
}

impl From<&Strategy> for PyStrategyKind {
    fn from(strategy: &Strategy) -> Self {
        #[deny(clippy::wildcard_enum_match_arm)]
        match strategy {
            Strategy::SimpleStrategy { .. } => PyStrategyKind::Simple,
            Strategy::NetworkTopologyStrategy { .. } => PyStrategyKind::NetworkTopology,
            Strategy::LocalStrategy => PyStrategyKind::Local,
            Strategy::Other { .. } => PyStrategyKind::Other,
            _ => unreachable!("clippy testifies that the match is exhaustive"),
        }
    }
}

#[pymethods]
impl PyStrategy {
    #[getter]
    fn kind(&self) -> PyStrategyKind {
        PyStrategyKind::from(&self._inner)
    }

    #[getter]
    fn replication_factor<'py>(&self, py: Python<'py>) -> PyResult<Py<PyAny>> {
        match &self._inner {
            Strategy::SimpleStrategy {
                replication_factor, ..
            } => replication_factor.into_py_any(py),
            Strategy::LocalStrategy => 1.into_py_any(py),
            Strategy::NetworkTopologyStrategy {
                datacenter_repfactors,
            } => PyMappingProxy::new(
                py,
                datacenter_repfactors.iter().into_py_dict(py)?.as_mapping(),
            )
            .into_py_any(py),
            _ => Ok(py.None()),
        }
    }

    #[getter]
    fn other_name(&self) -> Option<&str> {
        match &self._inner {
            Strategy::Other { name, .. } => Some(name),
            _ => None,
        }
    }

    #[getter]
    fn other_data<'py>(&self, py: Python<'py>) -> PyResult<Option<Bound<'py, PyMappingProxy>>> {
        match &self._inner {
            Strategy::Other { data, .. } => Ok(Some(PyMappingProxy::new(
                py,
                data.iter().into_py_dict(py)?.as_mapping(),
            ))),
            _ => Ok(None),
        }
    }

    fn __repr__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyString>> {
        PyString::from_fmt(py, format_args!("{:?}", self._inner))
    }
}

#[pyclass(name = "ColumnKind", frozen, eq, eq_int, skip_from_py_object)]
#[derive(Clone, PartialEq)]
pub(crate) enum PyColumnKind {
    Regular,
    Static,
    Clustering,
    PartitionKey,
}

impl From<&ColumnKind> for PyColumnKind {
    fn from(kind: &ColumnKind) -> Self {
        #[deny(clippy::wildcard_enum_match_arm)]
        match kind {
            ColumnKind::Regular => PyColumnKind::Regular,
            ColumnKind::Static => PyColumnKind::Static,
            ColumnKind::Clustering => PyColumnKind::Clustering,
            ColumnKind::PartitionKey => PyColumnKind::PartitionKey,
            _ => unreachable!("clippy testifies that the match is exhaustive"),
        }
    }
}

#[pyclass(name = "Column", frozen, skip_from_py_object)]
#[derive(Clone)]
pub(crate) struct PyColumn {
    _inner: Column,
    #[pyo3(get)]
    kind: Py<PyColumnKind>,
}

impl TryFrom<&Column> for PyColumn {
    type Error = PyErr;
    fn try_from(value: &Column) -> Result<Self, Self::Error> {
        Python::attach(|py| {
            Ok(Self {
                kind: Py::new(py, PyColumnKind::from(&value.kind))?,
                _inner: value.clone(),
            })
        })
    }
}

#[pymethods]
impl PyColumn {
    fn __repr__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyString>> {
        PyString::from_fmt(
            py,
            format_args!(
                "Column(typ='{:?}', kind={:?})",
                self._inner.typ, self._inner.kind
            ),
        )
    }
}

#[pyclass(name = "Table", frozen, skip_from_py_object)]
pub(crate) struct PyTable {
    _inner: Table,
    partitioner: OnceLock<Option<Py<PyString>>>,
    /// Invariant: Holds all known `PyColumn`s of the table.
    columns: Py<PyDict>,
    /// Partition key columns of this table.
    ///
    /// Invariant: Always has partition key `PyColumn`s.
    partition_key: Py<PyDict>,
    /// Clustering key columns of this table.
    ///
    /// Invariant: Always has clustering key `PyColumn`s.
    clustering_key: Py<PyDict>,
}

impl TryFrom<Table> for PyTable {
    type Error = PyErr;

    fn try_from(inner: Table) -> Result<Self, Self::Error> {
        Python::attach(|py| {
            let py_cols = PyDict::new(py);
            let py_partition_key = PyDict::new(py);
            let py_clustering_key = PyDict::new(py);

            // Initialize columns dictionary
            for (name, column) in inner.columns.iter() {
                py_cols.set_item(name, PyColumn::try_from(column)?)?;
            }

            // Reuse the same columns for partition and clustering keys
            for name in &inner.partition_key {
                py_partition_key.set_item(name, py_cols.get_item(name)?)?;
            }

            for name in &inner.clustering_key {
                py_clustering_key.set_item(name, py_cols.get_item(name)?)?;
            }

            Ok(Self {
                _inner: inner,
                partitioner: OnceLock::new(),
                columns: py_cols.unbind(),
                partition_key: py_partition_key.unbind(),
                clustering_key: py_clustering_key.unbind(),
            })
        })
    }
}

#[pymethods]
impl PyTable {
    #[getter]
    fn columns<'py>(&self, py: Python<'py>) -> Bound<'py, PyMappingProxy> {
        PyMappingProxy::new(py, self.columns.bind(py).as_mapping())
    }

    #[getter]
    fn partition_key<'py>(&self, py: Python<'py>) -> Bound<'py, PyMappingProxy> {
        PyMappingProxy::new(py, self.partition_key.bind(py).as_mapping())
    }

    #[getter]
    fn clustering_key<'py>(&self, py: Python<'py>) -> Bound<'py, PyMappingProxy> {
        PyMappingProxy::new(py, self.clustering_key.bind(py).as_mapping())
    }

    #[getter]
    fn partitioner<'py>(&self, py: Python<'py>) -> &Option<Py<PyString>> {
        self.partitioner.get_or_init_py_attached(py, || {
            self._inner
                .partitioner
                .as_ref()
                .map(|p| PyString::new(py, p).unbind())
        })
    }

    fn __repr__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyString>> {
        PyString::from_fmt(
            py,
            format_args!(
                "Table(columns={}, partition_key={:?}, clustering_key={:?})",
                self._inner.columns.len(),
                self._inner.partition_key,
                self._inner.clustering_key,
            ),
        )
    }
}

#[pyclass(name = "Keyspace", frozen)]
pub(crate) struct PyKeyspace {
    pub(crate) _inner: Keyspace,
    pub(crate) strategy: OnceLock<Py<PyStrategy>>,
    /// Tables in this keyspace.
    pub(crate) tables: Cache<String, PyTable>,
}

#[pymethods]
impl PyKeyspace {
    #[getter]
    fn strategy<'py>(&self, py: Python<'py>) -> PyResult<Py<PyStrategy>> {
        match self.strategy.get() {
            Some(s) => Ok(s.clone_ref(py)),
            None => {
                let py_ref = Py::new(py, PyStrategy::from(self._inner.strategy.clone()))?;
                let strategy = self.strategy.get_or_init_py_attached(py, || py_ref);
                Ok(strategy.clone_ref(py))
            }
        }
    }

    #[getter]
    fn tables<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyMappingProxy>> {
        self.tables.get_or_init_python_mapping(py, || {
            self._inner
                .tables
                .iter()
                .map(|(name, table)| {
                    let py_table = PyTable::try_from(table.clone());
                    (name.clone(), py_table.and_then(|t| Py::new(py, t)))
                })
                .collect()
        })
    }

    fn __repr__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyString>> {
        PyString::from_fmt(
            py,
            format_args!(
                "Keyspace(strategy={:?}, tables={}, views={}, udts={})",
                self._inner.strategy,
                self._inner.tables.len(),
                self._inner.views.len(),
                self._inner.user_defined_types.len(),
            ),
        )
    }
}

#[pymodule]
pub(crate) fn metadata(_py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    // Metadata classes
    module.add_class::<PyColumn>()?;
    module.add_class::<PyColumnKind>()?;
    module.add_class::<PyTable>()?;
    module.add_class::<PyKeyspace>()?;
    module.add_class::<PyStrategy>()?;
    module.add_class::<PyStrategyKind>()?;

    Ok(())
}
