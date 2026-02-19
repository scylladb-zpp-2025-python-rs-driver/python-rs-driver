use std::sync::Arc;

use pyo3::{prelude::*, types::PyString};
use scylla::cluster;
use uuid::Uuid;

use crate::routing::sharding::Sharder;
use scylla::routing::Shard;

#[pyclass]
pub(crate) struct Node {
    pub(crate) _inner: Arc<cluster::Node>,
}

#[pymethods]
impl Node {
    #[getter]
    fn host_id(&self) -> Uuid {
        self._inner.host_id
    }

    #[getter]
    fn address(&self) -> String {
        self._inner.address.to_string()
    }

    #[getter]
    fn datacenter<'py>(&self, py: Python<'py>) -> Option<Bound<'py, PyString>> {
        self._inner
            .datacenter
            .as_ref()
            .map(|dc| PyString::new(py, dc))
    }

    #[getter]
    fn rack<'py>(&self, py: Python<'py>) -> Option<Bound<'py, PyString>> {
        self._inner
            .rack
            .as_ref()
            .map(|rack| PyString::new(py, rack))
    }

    #[getter]
    fn node_shard(&self) -> NodeShard {
        NodeShard {
            _inner: (self._inner.host_id, None),
        }
    }

    fn __repr__(&self) -> String {
        format!(
            "Node(host_id='{}', address='{}')",
            self._inner.host_id, self._inner.address
        )
    }

    fn sharder(&self) -> Option<Sharder> {
        self._inner.sharder().map(Sharder::from_rust)
    }

    fn is_connected(&self) -> bool {
        self._inner.is_connected()
    }

    fn is_enabled(&self) -> bool {
        self._inner.is_enabled()
    }
}

#[pyclass(frozen, skip_from_py_object)]
#[derive(Clone)]
pub(crate) struct NodeShard {
    pub(crate) _inner: (Uuid, Option<Shard>),
}

#[pymethods]
impl NodeShard {
    #[new]
    fn new(host_id: Uuid, shard: Option<u32>) -> Self {
        NodeShard {
            _inner: (host_id, shard),
        }
    }

    #[getter]
    fn host_id(&self) -> Uuid {
        self._inner.0
    }

    #[getter]
    fn shard(&self) -> Option<u32> {
        self._inner.1
    }

    fn __repr__(&self) -> String {
        format!(
            "NodeShard(host_id='{}', shard={:?})",
            self._inner.0, self._inner.1
        )
    }
}
