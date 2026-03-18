use std::sync::{Arc, atomic::AtomicBool};

use pyo3::{
    IntoPyObjectExt,
    exceptions::PyRuntimeError,
    prelude::*,
    types::{PyDict, PyList, PyMappingProxy, PyString},
};
use scylla::cluster::ClusterState;
use uuid::Uuid;

use crate::{cluster::node::PyNode, routing::PyToken, serialize::value_list::PyValueList};

#[pyclass(name = "ClusterState", frozen, skip_from_py_object)]
pub(crate) struct PyClusterState {
    pub(crate) _inner: Arc<ClusterState>,
    /// Stores a tuple of cached nodes as PyDict
    /// with erased type information and cache flag.
    ///
    /// Cache flag signals if all nodes are cached.
    ///
    /// Invariant: `PyDict<Uuid, Py<PyNode>>`
    pub(crate) known_nodes: (Py<PyDict>, AtomicBool),
    /// Stores a tuple of cached keyspaces as PyDict
    /// with erased type information and cache flag.
    ///
    /// Cache flag signals if all keyspaces are cached.
    ///
    /// Invariant: `PyDict<Py<PyString>, Py<PyKeyspace>>`
    #[expect(unused)]
    keyspaces: (Py<PyDict>, AtomicBool),
}

impl PyClusterState {
    pub(crate) fn new<'py>(_inner: Arc<ClusterState>, py: Python<'py>) -> PyResult<Self> {
        Ok(Self {
            _inner,
            known_nodes: (PyDict::new(py).unbind(), AtomicBool::new(false)),
            keyspaces: (PyDict::new(py).unbind(), AtomicBool::new(false)),
        })
    }

    pub(crate) fn get_or_init_node_from_cache<'py>(
        &self,
        py: Python<'py>,
        node_id: &Uuid,
    ) -> PyResult<Option<Py<PyAny>>> {
        let cache = self.known_nodes.0.bind(py);
        let cache_flag = &self.known_nodes.1;
        match cache.get_item(node_id)? {
            Some(node) => Ok(Some(node.unbind().clone_ref(py))), // Node present in cache
            None => {
                if cache_flag.load(std::sync::atomic::Ordering::Relaxed) {
                    // O(1) return when node is not in cache and all nodes are known.
                    return Ok(None);
                }
                let Some(node) = self._inner.known_peers.get(node_id) else {
                    // Return None when node is not known.
                    return Ok(None);
                };
                let py_node = PyNode {
                    _inner: node.clone(),
                }
                .into_py_any(py)?;
                cache.set_item(node_id, py_node.clone_ref(py))?;
                Ok(Some(py_node))
            }
        }
    }
}

#[pymethods]
impl PyClusterState {
    #[getter]
    fn get_nodes_info<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyMappingProxy>> {
        if self
            .known_nodes
            .1
            .load(std::sync::atomic::Ordering::Relaxed)
        {
            return Ok(PyMappingProxy::new(
                py,
                self.known_nodes.0.bind(py).as_mapping(),
            ));
        }

        let cache = self.known_nodes.0.bind(py);

        for node in self._inner.get_nodes_info() {
            match cache.get_item(node.host_id)? {
                Some(_) => {} // Node already cached.
                None => {
                    let py_node = PyNode {
                        _inner: node.clone(),
                    }
                    .into_py_any(py)?;
                    cache.set_item(node.host_id, py_node)?;
                }
            }
        }
        self.known_nodes
            .1
            .store(true, std::sync::atomic::Ordering::Relaxed);
        Ok(PyMappingProxy::new(
            py,
            self.known_nodes.0.bind(py).as_mapping(),
        ))
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
                let py_node = self.get_or_init_node_from_cache(py, &node.0.host_id)?;
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
                let py_node = self.get_or_init_node_from_cache(py, &node.0.host_id)?;
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
