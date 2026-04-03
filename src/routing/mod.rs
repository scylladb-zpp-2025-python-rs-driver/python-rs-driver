use std::sync::Arc;

use pyo3::{
    IntoPyObjectExt,
    prelude::*,
    types::{PyDict, PyList, PyString},
};
use scylla::{
    cluster::ClusterState,
    frame::response::result::TableSpec,
    routing::{Shard, Token},
};

use crate::cluster::metadata::PyStrategy;

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
    _inner: Arc<ClusterState>,
    known_nodes: Py<PyDict>,
}

impl From<(Arc<ClusterState>, Py<PyDict>)> for PyReplicaLocator {
    fn from(inner: (Arc<ClusterState>, Py<PyDict>)) -> Self {
        PyReplicaLocator {
            _inner: inner.0,
            known_nodes: inner.1,
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
        let replica_set = self._inner.replica_locator().replicas_for_token(
            token._inner,
            &strategy._inner,
            datacenter,
            &table_spec,
        );
        let Some((node, shard)) = replica_set.into_iter().next() else {
            return Ok(None);
        };
        Ok(Some((
            self.known_nodes
                .bind(py)
                .get_item(node.host_id)?
                .expect("Node should be in cache")
                .into_py_any(py)?
                .clone_ref(py),
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
        let replica_set = self._inner.replica_locator().replicas_for_token(
            token._inner,
            &strategy._inner,
            datacenter,
            &table_spec,
        );
        let list = PyList::empty(py);
        for (node, shard) in replica_set {
            list.append((
                self.known_nodes
                    .bind(py)
                    .get_item(node.host_id)?
                    .expect("Node should be in cache")
                    .into_py_any(py)?
                    .clone_ref(py),
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
            .replica_locator()
            .unique_nodes_in_global_ring()
            .iter()
            .map(|n| {
                Ok::<_, PyErr>(
                    self.known_nodes
                        .bind(py)
                        .get_item(n.host_id)?
                        .expect("Node should be in cache")
                        .into_py_any(py)?
                        .clone_ref(py),
                )
            });
        let list = PyList::empty(py);
        for node in nodes_iter {
            list.append(node?)?;
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
            .replica_locator()
            .unique_nodes_in_datacenter_ring(datacenter)
        else {
            return Ok(None);
        };
        let nodes_iter = unique_nodes.iter().map(|n| {
            Ok::<_, PyErr>(
                self.known_nodes
                    .bind(py)
                    .get_item(n.host_id)?
                    .expect("Node should be in cache")
                    .into_py_any(py)?
                    .clone_ref(py),
            )
        });
        let list = PyList::empty(py);
        for node in nodes_iter {
            list.append(node?)?;
        }
        Ok(Some(list))
    }

    #[getter]
    fn datacenter_names<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        PyList::new(py, self._inner.replica_locator().datacenter_names())
    }

    fn __repr__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyString>> {
        PyString::from_fmt(
            py,
            format_args!(
                "ReplicaLocator(ring_len={}, datacenters={:?})",
                self._inner.replica_locator().ring().len(),
                self._inner.replica_locator().datacenter_names(),
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
