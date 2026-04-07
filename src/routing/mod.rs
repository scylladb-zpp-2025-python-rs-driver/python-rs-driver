use pyo3::{
    prelude::*,
    types::{PyList, PyString},
};
use scylla::{
    frame::response::result::TableSpec,
    routing::{Shard, Token},
};

use crate::cluster::{metadata::PyStrategy, state::PyClusterState};

#[pyclass(name = "Token", frozen, from_py_object)]
#[derive(Clone)]
pub(crate) struct PyToken {
    pub(crate) _inner: Token,
}

#[pymethods]
impl PyToken {
    #[new]
    fn new(value: i64) -> Self {
        Self {
            _inner: Token::new(value),
        }
    }

    #[getter]
    fn value(&self) -> i64 {
        self._inner.value()
    }

    fn __repr__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyString>> {
        PyString::from_fmt(py, format_args!("Token({})", self._inner.value()))
    }

    fn __eq__(&self, other: &PyToken) -> bool {
        self._inner == other._inner
    }

    fn __hash__(&self) -> i64 {
        self._inner.value()
    }
}

#[pyclass(name = "ReplicaLocator", frozen, skip_from_py_object)]
pub(crate) struct PyReplicaLocator {
    _inner: Py<PyClusterState>,
}

impl From<PyRef<'_, PyClusterState>> for PyReplicaLocator {
    fn from(inner: PyRef<'_, PyClusterState>) -> Self {
        PyReplicaLocator {
            _inner: inner.into(),
        }
    }
}

#[pymethods]
impl PyReplicaLocator {
    #[pyo3(
        signature = (token, strategy, keyspace, table, datacenter=None,)
    )]
    fn primary_replica_for_token<'py>(
        &self,
        token: &PyToken,
        strategy: &PyStrategy,
        keyspace: &str,
        table: &str,
        datacenter: Option<&str>,
        py: Python<'py>,
    ) -> PyResult<Option<(Py<PyAny>, Shard)>> {
        let table_spec = TableSpec::borrowed(keyspace, table);
        let replica_set = self
            ._inner
            .bind(py)
            .get()
            ._inner
            .replica_locator()
            .replicas_for_token(token._inner, &strategy._inner, datacenter, &table_spec);
        let Some((node, shard)) = replica_set.into_iter().next() else {
            return Ok(None);
        };
        Ok(Some((
            self._inner
                .bind(py)
                .get()
                .get_or_init_node_from_cache(py, &node.host_id)?
                .expect("Node should be known by the driver"),
            shard,
        )))
    }

    #[pyo3(
        signature = (token, strategy, keyspace, table, datacenter=None,)
    )]
    fn all_replicas_for_token<'py>(
        &self,
        token: &PyToken,
        strategy: &PyStrategy,
        keyspace: &str,
        table: &str,
        datacenter: Option<&str>,
        py: Python<'py>,
    ) -> PyResult<Bound<'py, PyList>> {
        let table_spec = TableSpec::borrowed(keyspace, table);
        let replica_set = self
            ._inner
            .bind(py)
            .get()
            ._inner
            .replica_locator()
            .replicas_for_token(token._inner, &strategy._inner, datacenter, &table_spec);
        let list = PyList::empty(py);
        for (node, shard) in replica_set {
            list.append((
                self._inner
                    .bind(py)
                    .get()
                    .get_or_init_node_from_cache(py, &node.host_id)?
                    .expect("Node should be known by the driver"),
                shard,
            ))?;
        }
        Ok(list)
    }

    fn unique_token_owning_nodes_in_cluster<'py>(
        &self,
        py: Python<'py>,
    ) -> PyResult<Bound<'py, PyList>> {
        let nodes_iter = self
            ._inner
            .bind(py)
            .get()
            ._inner
            .replica_locator()
            .unique_nodes_in_global_ring()
            .iter()
            .map(|n| {
                self._inner
                    .bind(py)
                    .get()
                    .get_or_init_node_from_cache(py, &n.host_id)
            });
        let list = PyList::empty(py);
        for node in nodes_iter {
            list.append(node?.expect("Node should be known by the driver"))?;
        }
        Ok(list)
    }

    fn unique_token_owning_nodes_in_datacenter<'py>(
        &self,
        datacenter: &str,
        py: Python<'py>,
    ) -> PyResult<Option<Bound<'py, PyList>>> {
        let Some(unique_nodes) = self
            ._inner
            .bind(py)
            .get()
            ._inner
            .replica_locator()
            .unique_nodes_in_datacenter_ring(datacenter)
        else {
            return Ok(None);
        };
        let nodes_iter = unique_nodes.iter().map(|n| {
            self._inner
                .bind(py)
                .get()
                .get_or_init_node_from_cache(py, &n.host_id)
        });
        let list = PyList::empty(py);
        for node in nodes_iter {
            list.append(node?.expect("Node should be known by the driver"))?;
        }
        Ok(Some(list))
    }

    #[getter]
    fn datacenter_names<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        PyList::new(
            py,
            self._inner
                .bind(py)
                .get()
                ._inner
                .replica_locator()
                .datacenter_names(),
        )
    }

    fn __repr__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyString>> {
        PyString::from_fmt(
            py,
            format_args!(
                "ReplicaLocator(ring_len={}, datacenters={:?})",
                self._inner
                    .bind(py)
                    .get()
                    ._inner
                    .replica_locator()
                    .ring()
                    .len(),
                self._inner
                    .bind(py)
                    .get()
                    ._inner
                    .replica_locator()
                    .datacenter_names(),
            ),
        )
    }
}

#[pymodule]
pub(crate) fn routing(_py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<PyToken>()?;
    module.add_class::<PyReplicaLocator>()?;
    Ok(())
}
