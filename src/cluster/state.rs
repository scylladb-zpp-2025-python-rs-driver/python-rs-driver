use std::sync::Arc;

use pyo3::{
    IntoPyObjectExt,
    exceptions::PyRuntimeError,
    prelude::*,
    types::{PyDict, PyList, PyString},
};
use scylla::cluster::ClusterState;

use crate::{cluster::node::PyNode, routing::PyToken, serialize::value_list::PyValueList};

#[pyclass(name = "ClusterState", frozen, skip_from_py_object)]
pub(crate) struct PyClusterState {
    pub(crate) _inner: Arc<ClusterState>,
    /// Stores cached nodes as PyDict with erased type information.
    ///
    /// Known nodes are cached on PyClusterState creation.
    ///
    /// Invariant: `PyDict<Uuid, Py<PyNode>>`
    known_nodes: Py<PyDict>,
}

impl PyClusterState {
    #[expect(unused)]
    pub(crate) fn new<'py>(_inner: Arc<ClusterState>, py: Python<'py>) -> PyResult<Self> {
        let known_nodes = PyDict::new(py);
        for node in _inner.get_nodes_info().iter() {
            known_nodes.set_item(
                node.host_id,
                PyNode {
                    _inner: Arc::clone(node),
                }
                .into_py_any(py)?,
            )?;
        }
        Ok(Self {
            _inner,
            known_nodes: known_nodes.unbind(),
        })
    }
}

#[pymethods]
impl PyClusterState {
    #[getter]
    fn get_nodes_info<'py>(&self, py: Python<'py>) -> Py<PyDict> {
        self.known_nodes.clone_ref(py)
    }

    fn compute_token(
        &self,
        keyspace: &str,
        table: &str,
        partition_key: PyValueList,
    ) -> PyResult<PyToken> {
        self._inner
            .compute_token(keyspace, table, &partition_key)
            .map(|t| PyToken { _inner: t })
            .map_err(|e| PyErr::new::<PyRuntimeError, _>(format!("Error computing token: {}", e)))
    }

    fn get_token_endpoints<'py>(
        &self,
        keyspace: &str,
        table: &str,
        token: &PyToken,
        py: Python<'py>,
    ) -> PyResult<Bound<'py, PyList>> {
        let py_nodes_sequence = self
            ._inner
            .get_token_endpoints(keyspace, table, token._inner)
            .into_iter()
            .map(|node| {
                let py_node = self
                    .known_nodes
                    .bind(py)
                    .get_item(node.0.host_id)?
                    .expect("Node is cached")
                    .into_py_any(py)?;
                Ok::<_, PyErr>((py_node, node.1))
            });
        let list = PyList::empty(py);
        for py_node in py_nodes_sequence {
            list.append(py_node?)?;
        }
        Ok(list)
    }

    fn get_endpoints<'py>(
        &self,
        keyspace: &str,
        table: &str,
        partition_key: PyValueList,
        py: Python<'py>,
    ) -> PyResult<Bound<'py, PyList>> {
        let py_nodes_sequence = self
            ._inner
            .get_endpoints(keyspace, table, &partition_key)
            .map_err(|e| {
                PyErr::new::<PyRuntimeError, _>(format!("Error getting endpoints: {}", e))
            })?
            .into_iter()
            .map(|node| {
                let py_node = self
                    .known_nodes
                    .bind(py)
                    .get_item(node.0.host_id)?
                    .expect("Node is cached")
                    .into_py_any(py)?;
                Ok::<_, PyErr>((py_node, node.1))
            });
        let list = PyList::empty(py);
        for py_node in py_nodes_sequence {
            list.append(py_node?)?;
        }
        Ok(list)
    }

    fn __repr__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyString>> {
        PyString::from_fmt(
            py,
            format_args!(
                "ClusterState(nodes={}, keyspaces={})",
                self._inner.get_nodes_info().len(),
                self._inner.keyspaces_iter().count()
            ),
        )
    }
}
