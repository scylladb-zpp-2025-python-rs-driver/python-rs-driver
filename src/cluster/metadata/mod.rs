use std::sync::OnceLock;

use pyo3::{
    IntoPyObjectExt,
    prelude::*,
    types::{IntoPyDict, PyDict, PyString},
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

#[pymethods]
impl PyStrategy {
    #[deny(clippy::wildcard_enum_match_arm)]
    #[getter]
    fn kind(&self) -> PyStrategyKind {
        match &self._inner {
            Strategy::SimpleStrategy { .. } => PyStrategyKind::Simple,
            Strategy::NetworkTopologyStrategy { .. } => PyStrategyKind::NetworkTopology,
            Strategy::LocalStrategy => PyStrategyKind::Local,
            Strategy::Other { .. } => PyStrategyKind::Other,
            _ => unreachable!(),
        }
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
            } => datacenter_repfactors
                .iter()
                .into_py_dict(py)?
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
    fn other_data<'py>(&self, py: Python<'py>) -> PyResult<Option<Bound<'py, PyDict>>> {
        match &self._inner {
            Strategy::Other { data, .. } => Some(data.iter().into_py_dict(py)).transpose(),
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
            _ => unreachable!(),
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
#[derive(Clone)]
pub(crate) struct PyTable {
    _inner: Table,
    #[pyo3(get)]
    columns: Py<PyDict>,
    #[pyo3(get)]
    partition_key: Py<PyDict>,
    #[pyo3(get)]
    clustering_key: Py<PyDict>,
    #[pyo3(get)]
    partitioner: Option<Py<PyString>>,
}

impl PyTable {
    pub(crate) fn new(inner: Table, py: Python) -> PyResult<Self> {
        let columns = PyDict::new(py);
        for (name, col) in inner.columns.iter() {
            columns.set_item(name, PyColumn::new(col.clone(), py)?)?;
        }

        let partition_key = PyDict::new(py);
        for col_name in inner.partition_key.iter() {
            let py_column = columns
                .get_item(col_name)?
                .expect("Partition key colums are always present");
            partition_key.set_item(col_name, py_column.into_py_any(py)?.clone_ref(py))?
        }

        let clustering_key = PyDict::new(py);
        for col_name in inner.clustering_key.iter() {
            let py_column = columns
                .get_item(col_name)?
                .expect("Clustering key colums are always present");
            clustering_key.set_item(col_name, py_column.into_py_any(py)?.clone_ref(py))?
        }

        Ok(Self {
            columns: Py::from(columns),
            partition_key: Py::from(partition_key),
            clustering_key: Py::from(clustering_key),
            partitioner: inner
                .partitioner
                .as_ref()
                .map(|p| Py::from(PyString::new(py, p))),
            _inner: inner,
        })
    }
}

#[pymethods]
impl PyTable {
    // #[getter]
    // fn columns<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
    //     self._inner
    //         .columns
    //         .iter()
    //         .map(|(name, col)| (PyString::new(py, name), PyColumn::new(col.clone(), py)?))
    //         .into_py_dict(py)
    // }

    // #[getter]
    // fn partition_key<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
    //     let py_cols = self._inner.partition_key.iter().filter_map(|col_name| {
    //         self._inner
    //             .columns
    //             .get(col_name)
    //             .map(|col| PyColumn::new(col.clone(), py))
    //     });
    //     let list = PyList::empty(py);
    //     for py_col in py_cols {
    //         list.append(py_col)?;
    //     }

    //     Ok(list)
    // }

    // #[getter]
    // fn clustering_key<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
    //     let py_cols = self._inner.clustering_key.iter().filter_map(|col_name| {
    //         self._inner
    //             .columns
    //             .get(col_name)
    //             .map(|col| PyColumn::new(col.clone(), py))
    //     });
    //     let list = PyList::empty(py);
    //     for py_col in py_cols {
    //         list.append(py_col)?;
    //     }

    //     Ok(list)
    // }

    // #[getter]
    // fn partitioner<'py>(&self, py: Python<'py>) -> Option<Bound<'py, PyString>> {
    //     self._inner
    //         .partitioner
    //         .as_ref()
    //         .map(|p| PyString::new(py, p))
    // }

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
#[derive(Clone)]
pub(crate) struct PyMaterializedView {
    _inner: MaterializedView,
    #[pyo3(get)]
    base_table_name: Py<PyString>,
    #[pyo3(get)]
    columns: Py<PyDict>,
    #[pyo3(get)]
    partition_key: Py<PyDict>,
    #[pyo3(get)]
    clustering_key: Py<PyDict>,
    #[pyo3(get)]
    partitioner: Option<Py<PyString>>,
}

impl PyMaterializedView {
    pub(crate) fn new(inner: MaterializedView, py: Python) -> PyResult<Self> {
        let base_table_name = PyString::new(py, &inner.base_table_name);

        let columns = PyDict::new(py);
        for (name, col) in inner.view_metadata.columns.iter() {
            columns.set_item(name, PyColumn::new(col.clone(), py)?)?;
        }

        let partition_key = PyDict::new(py);
        for col_name in inner.view_metadata.partition_key.iter() {
            let py_column = columns
                .get_item(col_name)?
                .expect("Partition key colums are always present");
            partition_key.set_item(col_name, py_column.into_py_any(py)?.clone_ref(py))?
        }

        let clustering_key = PyDict::new(py);
        for col_name in inner.view_metadata.clustering_key.iter() {
            let py_column = columns
                .get_item(col_name)?
                .expect("Clustering key colums are always present");
            clustering_key.set_item(col_name, py_column.into_py_any(py)?.clone_ref(py))?
        }

        Ok(Self {
            base_table_name: Py::from(base_table_name),
            columns: Py::from(columns),
            partition_key: Py::from(partition_key),
            clustering_key: Py::from(clustering_key),
            partitioner: inner
                .view_metadata
                .partitioner
                .as_ref()
                .map(|p| Py::from(PyString::new(py, p))),
            _inner: inner,
        })
    }
}

#[pymethods]
impl PyMaterializedView {
    // #[getter]
    // fn base_table_name<'py>(&self, py: Python<'py>) -> Bound<'py, PyString> {
    //     PyString::new(py, &self._inner.base_table_name)
    // }

    // #[getter]
    // fn columns<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
    //     self._inner
    //         .view_metadata
    //         .columns
    //         .iter()
    //         .map(|(name, col)| (PyString::new(py, name), PyColumn::new(col.clone(), py)?))
    //         .into_py_dict(py)
    // }

    // #[getter]
    // fn partition_key<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
    //     let py_cols = self
    //         ._inner
    //         .view_metadata
    //         .partition_key
    //         .iter()
    //         .filter_map(|col_name| {
    //             self._inner
    //                 .view_metadata
    //                 .columns
    //                 .get(col_name)
    //                 .map(|col| PyColumn::new(col.clone(), py))
    //         });
    //     let list = PyList::empty(py);
    //     for py_col in py_cols {
    //         list.append(py_col)?;
    //     }

    //     Ok(list)
    // }

    // #[getter]
    // fn clustering_key<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
    //     let py_cols = self
    //         ._inner
    //         .view_metadata
    //         .clustering_key
    //         .iter()
    //         .filter_map(|col_name| {
    //             self._inner
    //                 .view_metadata
    //                 .columns
    //                 .get(col_name)
    //                 .map(|col| PyColumn::new(col.clone(), py))
    //         });
    //     let list = PyList::empty(py);
    //     for py_col in py_cols {
    //         list.append(py_col)?;
    //     }

    //     Ok(list)
    // }

    // #[getter]
    // fn partitioner<'py>(&self, py: Python<'py>) -> Option<Bound<'py, PyString>> {
    //     self._inner
    //         .view_metadata
    //         .partitioner
    //         .as_ref()
    //         .map(|p| PyString::new(py, p))
    // }

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
    pub(crate) strategy: OnceLock<Py<PyAny>>,
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
            strategy: OnceLock::new(),
            _inner,
            tables: OnceLock::new(),
            views: OnceLock::new(),
        })
    }
}

#[pymethods]
impl PyKeyspace {
    #[getter]
    fn strategy<'py>(&self, py: Python<'py>) -> PyResult<Py<PyAny>> {
        match self.strategy.get() {
            Some(s) => s.clone_ref(py).into_py_any(py),
            None => {
                let s = PyStrategy::from(self._inner.strategy.clone()).into_py_any(py)?;
                self.strategy
                    .set(s.clone_ref(py))
                    .expect("Strategy wasn't set");
                Ok(s)
            }
        }
    }

    #[getter]
    fn tables<'py>(&self, py: Python<'py>) -> PyResult<Py<PyDict>> {
        if let Some(tables) = self.tables.get() {
            return Ok(tables.clone_ref(py));
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

        let tables: Py<PyDict> = tables.unbind();
        self.tables
            .set(tables.clone_ref(py))
            .expect("Tables weren't set");
        Ok(tables)
    }

    #[getter]
    fn views<'py>(&self, py: Python<'py>) -> PyResult<Py<PyDict>> {
        if let Some(views) = self.views.get() {
            return Ok(views.clone_ref(py));
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

        let views: Py<PyDict> = views.unbind();
        self.views
            .set(views.clone_ref(py))
            .expect("Views weren't set");
        Ok(views)
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
