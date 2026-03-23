use std::sync::{OnceLock, atomic::AtomicBool};

use pyo3::{
    IntoPyObjectExt,
    prelude::*,
    sync::OnceLockExt,
    types::{IntoPyDict, PyDict, PyMappingProxy, PyString},
};
use scylla::cluster::metadata::{Column, ColumnKind, Keyspace, MaterializedView, Strategy, Table};

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
    typ: Py<PyString>,
    #[pyo3(get)]
    kind: Py<PyColumnKind>,
}

impl PyColumn {
    pub(crate) fn new<'py>(inner: Column, py: Python<'py>) -> PyResult<Self> {
        Ok(Self {
            typ: PyString::from_fmt(py, format_args!("{:?}", inner.typ))?.unbind(),
            kind: Py::new(py, PyColumnKind::from(&inner.kind))?,
            _inner: inner,
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
    /// Stores a tuple of cached columns as PyDict
    /// with erased type information and cache flag.
    ///
    /// Cache flag signals if all columns are cached.
    /// It is set when all colums of the table are accessed.
    ///
    /// Invariant: `PyDict<Py<PyString>, Py<PyColumn>>`
    columns: (Py<PyDict>, AtomicBool),
    /// Columns that constitute partition key for this table.
    ///
    /// Once initialized, `columns` field dict contains
    /// columns that are part of the partition key.
    /// `partition_key` contains cloned references to those
    /// colums.
    ///
    /// Invariant: `PyDict<Py<PyString>, Py<PyColumn>>`
    partition_key: OnceLock<Py<PyDict>>,
    /// Columns that constitute clustering key for this table.
    ///
    /// Once initialized, `columns` field dict contains
    /// columns that are part of the clustering key.
    /// `clustering_key` contains cloned references to those
    /// colums.
    ///
    /// Invariant: `PyDict<Py<PyString>, Py<PyColumn>>`
    clustering_key: OnceLock<Py<PyDict>>,
}

impl PyTable {
    pub(crate) fn new(inner: Table, py: Python) -> PyResult<Self> {
        Ok(Self {
            columns: (PyDict::new(py).unbind(), AtomicBool::new(false)),
            partition_key: OnceLock::new(),
            clustering_key: OnceLock::new(),
            partitioner: OnceLock::new(),
            _inner: inner,
        })
    }
}

#[pymethods]
impl PyTable {
    #[getter]
    fn columns<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyMappingProxy>> {
        if self.columns.1.load(std::sync::atomic::Ordering::Relaxed) {
            return Ok(PyMappingProxy::new(
                py,
                self.columns.0.bind(py).as_mapping(),
            ));
        }

        let cache = self.columns.0.bind(py);

        for (name, column) in &self._inner.columns {
            match cache.get_item(name)? {
                Some(_) => {} // Column is present in cache
                None => {
                    let py_column = PyColumn::new(column.clone(), py)?.into_py_any(py)?;
                    cache.set_item(name, py_column)?;
                }
            }
        }

        self.columns
            .1
            .store(true, std::sync::atomic::Ordering::Relaxed);

        Ok(PyMappingProxy::new(
            py,
            self.columns.0.bind(py).as_mapping(),
        ))
    }

    #[getter]
    fn partition_key<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyMappingProxy>> {
        if let Some(partition_key) = self.partition_key.get() {
            return Ok(PyMappingProxy::new(py, partition_key.bind(py).as_mapping()));
        }

        let cache = self.columns.0.bind(py);

        let partition_key = PyDict::new(py);
        let py_partition_key_iter = self
            ._inner
            .partition_key
            .iter()
            .map(|pk_column| (pk_column, cache.get_item(pk_column)));
        for (name, cache_column) in py_partition_key_iter {
            let cache_column = cache_column?;
            match cache_column {
                Some(py_column) => {
                    // Column present in cache, partition_key is initizalized with cloned reference
                    partition_key.set_item(name, py_column.into_py_any(py)?)?;
                }
                None => {
                    // Column reference is missing in cache
                    let column = self
                        ._inner
                        .columns
                        .get(name)
                        .expect("Columns are guaranteed to be present");
                    let py_column = PyColumn::new(column.clone(), py)?.into_py_any(py)?;
                    cache.set_item(name, py_column.clone_ref(py))?;
                    partition_key.set_item(name, py_column)?;
                }
            }
        }

        let dict = self
            .partition_key
            .get_or_init_py_attached(py, || partition_key.unbind());

        Ok(PyMappingProxy::new(py, dict.bind(py).as_mapping()))
    }

    #[getter]
    fn clustering_key<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyMappingProxy>> {
        if let Some(clustering_key) = self.clustering_key.get() {
            return Ok(PyMappingProxy::new(
                py,
                clustering_key.bind(py).as_mapping(),
            ));
        }

        let cache = self.columns.0.bind(py);

        let clustering_key = PyDict::new(py);
        let py_clustering_key_iter = self
            ._inner
            .clustering_key
            .iter()
            .map(|pk_column| (pk_column, cache.get_item(pk_column)));
        for (name, cache_column) in py_clustering_key_iter {
            let cache_column = cache_column?;
            match cache_column {
                Some(py_column) => {
                    // Column present in cache, clustering_key is initizalized with cloned reference
                    clustering_key.set_item(name, py_column.into_py_any(py)?)?;
                }
                None => {
                    // Column reference is missing in cache
                    let column = self
                        ._inner
                        .columns
                        .get(name)
                        .expect("Columns are guaranteed to be present");
                    let py_column = PyColumn::new(column.clone(), py)?.into_py_any(py)?;
                    cache.set_item(name, py_column.clone_ref(py))?;
                    clustering_key.set_item(name, py_column)?;
                }
            }
        }
        let dict = self
            .clustering_key
            .get_or_init_py_attached(py, || clustering_key.unbind());

        Ok(PyMappingProxy::new(py, dict.bind(py).as_mapping()))
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

#[pyclass(name = "MaterializedView", frozen, skip_from_py_object)]
pub(crate) struct PyMaterializedView {
    _inner: MaterializedView,
    base_table_name: OnceLock<Py<PyString>>,
    partitioner: OnceLock<Option<Py<PyString>>>,
    /// Stores a tuple of cached columns as PyDict
    /// with erased type information and cache flag.
    ///
    /// Cache flag signals if all columns are cached.
    /// It is set when all colums of the table are accessed.
    ///
    /// Invariant: `PyDict<Py<PyString>, Py<PyColumn>>`
    columns: (Py<PyDict>, AtomicBool),
    /// Columns that constitute partition key for this view.
    ///
    /// Once initialized, `columns` field dict contains
    /// columns that are part of the partition key.
    /// `partition_key` contains cloned references to those
    /// colums.
    ///
    /// Invariant: `PyDict<Py<PyString>, Py<PyColumn>>`
    partition_key: OnceLock<Py<PyDict>>,
    /// Columns that constitute clustering key for this view.
    ///
    /// Once initialized, `columns` field dict contains
    /// columns that are part of the clustering key.
    /// `clustering_key` contains cloned references to those
    /// colums.
    ///
    /// Invariant: `PyDict<Py<PyString>, Py<PyColumn>>`
    clustering_key: OnceLock<Py<PyDict>>,
}

impl PyMaterializedView {
    pub(crate) fn new(inner: MaterializedView, py: Python) -> PyResult<Self> {
        Ok(Self {
            columns: (PyDict::new(py).unbind(), AtomicBool::new(false)),
            partition_key: OnceLock::new(),
            clustering_key: OnceLock::new(),
            partitioner: OnceLock::new(),
            base_table_name: OnceLock::new(),
            _inner: inner,
        })
    }
}

#[pymethods]
impl PyMaterializedView {
    #[getter]
    fn columns<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyMappingProxy>> {
        if self.columns.1.load(std::sync::atomic::Ordering::Relaxed) {
            return Ok(PyMappingProxy::new(
                py,
                self.columns.0.bind(py).as_mapping(),
            ));
        }

        let cache = self.columns.0.bind(py);

        for (name, column) in &self._inner.view_metadata.columns {
            match cache.get_item(name)? {
                Some(_) => {} // Column is present in cache
                None => {
                    let py_column = PyColumn::new(column.clone(), py)?.into_py_any(py)?;
                    cache.set_item(name, py_column)?;
                }
            }
        }

        self.columns
            .1
            .store(true, std::sync::atomic::Ordering::Relaxed);

        Ok(PyMappingProxy::new(
            py,
            self.columns.0.bind(py).as_mapping(),
        ))
    }

    #[getter]
    fn partition_key<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyMappingProxy>> {
        if let Some(partition_key) = self.partition_key.get() {
            return Ok(PyMappingProxy::new(py, partition_key.bind(py).as_mapping()));
        }

        let cache = self.columns.0.bind(py);

        let partition_key = PyDict::new(py);
        let py_partition_key_iter = self
            ._inner
            .view_metadata
            .partition_key
            .iter()
            .map(|pk_column| (pk_column, cache.get_item(pk_column)));
        for (name, cache_column) in py_partition_key_iter {
            let cache_column = cache_column?;
            match cache_column {
                Some(py_column) => {
                    // Column present in cache, partition_key is initizalized with cloned reference
                    partition_key.set_item(name, py_column.into_py_any(py)?)?;
                }
                None => {
                    // Column reference is missing in cache
                    let column = self
                        ._inner
                        .view_metadata
                        .columns
                        .get(name)
                        .expect("Columns are guaranteed to be present");
                    let py_column = PyColumn::new(column.clone(), py)?.into_py_any(py)?;
                    cache.set_item(name, py_column.clone_ref(py))?;
                    partition_key.set_item(name, py_column)?;
                }
            }
        }

        let dict = self
            .partition_key
            .get_or_init_py_attached(py, || partition_key.unbind());

        Ok(PyMappingProxy::new(py, dict.bind(py).as_mapping()))
    }

    #[getter]
    fn clustering_key<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyMappingProxy>> {
        if let Some(clustering_key) = self.clustering_key.get() {
            return Ok(PyMappingProxy::new(
                py,
                clustering_key.bind(py).as_mapping(),
            ));
        }

        let cache = self.columns.0.bind(py);

        let clustering_key = PyDict::new(py);
        let py_clustering_key_iter = self
            ._inner
            .view_metadata
            .clustering_key
            .iter()
            .map(|pk_column| (pk_column, cache.get_item(pk_column)));
        for (name, cache_column) in py_clustering_key_iter {
            let cache_column = cache_column?;
            match cache_column {
                Some(py_column) => {
                    // Column present in cache, clustering_key is initizalized with cloned reference
                    clustering_key.set_item(name, py_column.into_py_any(py)?)?;
                }
                None => {
                    // Column reference is missing in cache
                    let column = self
                        ._inner
                        .view_metadata
                        .columns
                        .get(name)
                        .expect("Columns are guaranteed to be present");
                    let py_column = PyColumn::new(column.clone(), py)?.into_py_any(py)?;
                    cache.set_item(name, py_column.clone_ref(py))?;
                    clustering_key.set_item(name, py_column)?;
                }
            }
        }

        let dict = self
            .clustering_key
            .get_or_init_py_attached(py, || clustering_key.unbind());

        Ok(PyMappingProxy::new(py, dict.bind(py).as_mapping()))
    }

    #[getter]
    fn partitioner<'py>(&self, py: Python<'py>) -> &Option<Py<PyString>> {
        self.partitioner.get_or_init_py_attached(py, || {
            self._inner
                .view_metadata
                .partitioner
                .as_ref()
                .map(|p| PyString::new(py, p).unbind())
        })
    }

    #[getter]
    fn base_table_name<'py>(&self, py: Python<'py>) -> &Py<PyString> {
        self.base_table_name.get_or_init_py_attached(py, || {
            PyString::new(py, self._inner.base_table_name.as_str()).unbind()
        })
    }

    fn __repr__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyString>> {
        PyString::from_fmt(
            py,
            format_args!(
                "MaterializedView(base_table='{}', columns={})",
                self._inner.base_table_name,
                self._inner.view_metadata.columns.len(),
            ),
        )
    }
}

#[pyclass(name = "Keyspace", frozen)]
pub(crate) struct PyKeyspace {
    pub(crate) _inner: Keyspace,
    /// Cached replication strategy for this keyspace (computed on first access).
    pub(crate) strategy: OnceLock<Py<PyStrategy>>,
    /// Cached tables in this keyspace (computed on first access).
    ///
    /// Invariant: `PyDict<Py<PyString>, Py<PyTable>>`
    pub(crate) tables: OnceLock<Py<PyDict>>,
    /// Cached views in this keyspace (computed on first access).
    ///
    /// Invariant: `PyDict<Py<PyString>, Py<PyMaterializedView>>`
    pub(crate) views: OnceLock<Py<PyDict>>,
}

impl PyKeyspace {
    pub(crate) fn new<'py>(py: Python<'py>, _inner: Keyspace) -> PyResult<Self> {
        let _ = py;
        Ok(Self {
            _inner,
            strategy: OnceLock::new(),
            tables: OnceLock::new(),
            views: OnceLock::new(),
        })
    }
}

#[pymethods]
impl PyKeyspace {
    #[getter]
    fn strategy<'py>(&self, py: Python<'py>) -> PyResult<Py<PyStrategy>> {
        match self.strategy.get() {
            Some(s) => Ok(s.clone_ref(py)),
            None => {
                let s = Py::new(py, PyStrategy::from(self._inner.strategy.clone()))?;
                let strategy = self
                    .strategy
                    .get_or_init_py_attached(py, || s.clone_ref(py));
                Ok(strategy.clone_ref(py))
            }
        }
    }

    #[getter]
    fn tables<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyMappingProxy>> {
        if let Some(tables) = self.tables.get() {
            return Ok(PyMappingProxy::new(py, tables.bind(py).as_mapping()));
        }

        let tables = PyDict::new(py);
        let py_tables_iter = self
            ._inner
            .tables
            .iter()
            .map(|(name, table)| (name, PyTable::new(table.clone(), py)));
        for (name, py_table) in py_tables_iter {
            tables.set_item(name, py_table?.into_py_any(py)?)?;
        }

        let dict = self.tables.get_or_init_py_attached(py, || tables.unbind());

        Ok(PyMappingProxy::new(py, dict.bind(py).as_mapping()))
    }

    #[getter]
    fn views<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyMappingProxy>> {
        if let Some(views) = self.views.get() {
            return Ok(PyMappingProxy::new(py, views.bind(py).as_mapping()));
        }

        let views = PyDict::new(py);
        let py_views_iter = self
            ._inner
            .views
            .iter()
            .map(|(name, view)| (name, PyMaterializedView::new(view.clone(), py)));
        for (name, py_view) in py_views_iter {
            views.set_item(name, py_view?.into_py_any(py)?)?;
        }
        let dict = self.views.get_or_init_py_attached(py, || views.unbind());

        Ok(PyMappingProxy::new(py, dict.bind(py).as_mapping()))
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
    module.add_class::<PyKeyspace>()?;
    module.add_class::<PyTable>()?;
    module.add_class::<PyColumn>()?;
    module.add_class::<PyColumnKind>()?;
    module.add_class::<PyMaterializedView>()?;
    module.add_class::<PyStrategy>()?;
    module.add_class::<PyStrategyKind>()?;
    Ok(())
}
