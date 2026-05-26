use std::{
    net::IpAddr,
    sync::{Arc, OnceLock},
};

use pyo3::{prelude::*, sync::OnceLockExt, types::PyString};
use uuid::Uuid;

use scylla::cluster::Node;

#[pyclass(name = "Node", frozen)]
pub(crate) struct PyNode {
    pub(crate) _inner: Arc<Node>,
    datacenter: OnceLock<Option<Py<PyString>>>,
    rack: OnceLock<Option<Py<PyString>>>,
}

impl From<Arc<Node>> for PyNode {
    fn from(_inner: Arc<Node>) -> Self {
        Self {
            _inner,
            datacenter: OnceLock::new(),
            rack: OnceLock::new(),
        }
    }
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
    fn datacenter<'py>(&self, py: Python<'py>) -> Option<Py<PyString>> {
        self.datacenter
            .get_or_init_py_attached(py, || {
                self._inner
                    .datacenter
                    .as_ref()
                    .map(|dc| PyString::new(py, dc).unbind())
            })
            .as_ref()
            .map(|dc| dc.clone_ref(py))
    }

    #[getter]
    fn rack<'py>(&self, py: Python<'py>) -> Option<Py<PyString>> {
        self.rack
            .get_or_init_py_attached(py, || {
                self._inner
                    .rack
                    .as_ref()
                    .map(|dc| PyString::new(py, dc).unbind())
            })
            .as_ref()
            .map(|rack| rack.clone_ref(py))
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
