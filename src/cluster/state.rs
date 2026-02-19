use std::sync::Arc;

use pyo3::{
    exceptions::PyRuntimeError,
    prelude::*,
    types::{IntoPyDict, PyDict, PyList, PyString},
};
use scylla::cluster;

use crate::{
    cluster::{metadata::Keyspace, node::Node},
    routing::{ReplicaLocator, Token},
    serialize::value::PyAnyWrapper,
};

pub(crate) type TableSpecOwned = (String, String);

#[pyclass(skip_from_py_object)]
#[derive(Clone)]
pub(crate) struct ClusterState {
    pub(crate) _inner: cluster::ClusterState,
}

#[pymethods]
impl ClusterState {
    fn get_keyspace(&self, keyspace: String) -> Option<Keyspace> {
        self._inner
            .get_keyspace(keyspace)
            .map(|ks| Keyspace { _inner: ks.clone() })
    }

    fn get_keyspaces<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        self._inner
            .keyspaces_iter()
            .map(|(name, ks)| (PyString::new(py, name), Keyspace { _inner: ks.clone() }))
            .into_py_dict(py)
    }

    fn get_nodes_info<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        PyList::new(
            py,
            self._inner.get_nodes_info().iter().map(|node| Node {
                _inner: Arc::clone(node),
            }),
        )
    }

    fn compute_token<'py>(
        &self,
        keyspace: String,
        table: String,
        partition_key: &Bound<'py, PyAny>,
    ) -> PyResult<Token> {
        let serializable_pk = PyAnyWrapper::new(partition_key);
        self._inner
            .compute_token(keyspace.as_str(), table.as_str(), &(serializable_pk,))
            .map(|t| Ok(Token { _inner: t }))
            .map_err(|e| PyErr::new::<PyRuntimeError, _>(format!("Error computing token: {}", e)))?
    }

    fn get_token_endpoints<'py>(
        &self,
        table_spec: TableSpecOwned,
        token: Token,
        py: Python<'py>,
    ) -> PyResult<Bound<'py, PyList>> {
        PyList::new(
            py,
            self._inner
                .get_token_endpoints(&table_spec.0, &table_spec.1, token._inner)
                .into_iter()
                .map(|(node, shard)| {
                    (
                        Node {
                            _inner: Arc::clone(&node),
                        },
                        shard,
                    )
                }),
        )
    }

    fn get_endpoints<'py>(
        &self,
        table_spec: TableSpecOwned,
        partition_key: &Bound<'py, PyAny>,
        py: Python<'py>,
    ) -> PyResult<Bound<'py, PyList>> {
        let serializable_pk = PyAnyWrapper::new(partition_key);
        PyList::new(
            py,
            self._inner
                .get_endpoints(&table_spec.0, &table_spec.1, &(serializable_pk,))
                .map_err(|e| {
                    PyErr::new::<PyRuntimeError, _>(format!("Error getting endpoints: {}", e))
                })?
                .iter()
                .map(|node| Node {
                    _inner: Arc::clone(&node.0),
                }),
        )
    }

    fn replica_locator(&self) -> ReplicaLocator {
        ReplicaLocator::from_rust(self._inner.replica_locator().clone())
    }

    fn __repr__(&self) -> String {
        format!(
            "ClusterState(nodes={}, keyspaces={})",
            self._inner.get_nodes_info().len(),
            self._inner.keyspaces_iter().count()
        )
    }
}
