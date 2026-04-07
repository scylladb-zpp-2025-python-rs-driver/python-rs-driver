use std::{net::IpAddr, sync::Arc};

use pyo3::{prelude::*, types::PyString};
use uuid::Uuid;

use scylla::{cluster::Node, routing::Shard};

#[pyclass(name = "Node", frozen)]
pub(crate) struct PyNode {
    pub(crate) _inner: Arc<Node>,
}

#[pymethods]
impl PyNode {
    #[getter]
    fn host_id(&self) -> Uuid {
        self._inner.host_id
    }

    #[getter]
    fn address(&self) -> (IpAddr, u16) {
        (self._inner.address.ip(), self._inner.address.port())
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

    fn __repr__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyString>> {
        PyString::from_fmt(
            py,
            format_args!(
                "Node(host_id='{}', address='{}')",
                self._inner.host_id, self._inner.address
            ),
        )
    }

    #[getter]
    fn nr_shards(&self) -> Option<usize> {
        self._inner.sharder().map(|s| s.nr_shards.get() as usize)
    }

    #[getter]
    fn connected(&self) -> bool {
        self._inner.is_connected()
    }

    #[getter]
    fn enabled(&self) -> bool {
        self._inner.is_enabled()
    }
}

#[pyclass(name = "NodeShard", frozen, skip_from_py_object)]
#[derive(Clone)]
pub(crate) struct PyNodeShard {
    pub(crate) _inner: (Uuid, Option<Shard>),
}

#[pymethods]
impl PyNodeShard {
    #[new]
    fn new(host_id: Uuid, shard: Option<Shard>) -> Self {
        PyNodeShard {
            _inner: (host_id, shard),
        }
    }

    #[getter]
    fn host_id(&self) -> Uuid {
        self._inner.0
    }

    #[getter]
    fn shard(&self) -> Option<Shard> {
        self._inner.1
    }

    fn __repr__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyString>> {
        PyString::from_fmt(
            py,
            format_args!(
                "NodeShard(host_id='{}', shard={:?})",
                self._inner.0, self._inner.1
            ),
        )
    }
}
