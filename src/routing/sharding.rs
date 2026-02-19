use crate::routing;
use pyo3::prelude::*;
use scylla::routing::Shard;

#[pyclass]
pub(crate) struct Sharder {
    _inner: scylla::routing::Sharder,
}

impl Sharder {
    #[allow(dead_code)]
    pub(crate) fn from_rust(inner: scylla::routing::Sharder) -> Self {
        Self { _inner: inner }
    }
}

#[pymethods]
impl Sharder {
    #[getter]
    fn nr_shards(&self) -> u16 {
        self._inner.nr_shards.get()
    }

    #[getter]
    fn msb_ignore(&self) -> u8 {
        self._inner.msb_ignore
    }

    fn shard_of(&self, token: routing::Token) -> Shard {
        self._inner.shard_of(token._inner)
    }

    fn shard_of_source_port(&self, source_port: u16) -> Shard {
        self._inner.shard_of_source_port(source_port)
    }

    fn draw_source_port_for_shard(&self, shard: Shard) -> u16 {
        self._inner.draw_source_port_for_shard(shard)
    }

    fn iter_source_ports_for_shard(&self, shard: Shard) -> Vec<u16> {
        self._inner.iter_source_ports_for_shard(shard).collect()
    }
}
