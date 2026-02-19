use std::sync::Arc;

use pyo3::{prelude::*, types::PyList};
use scylla::cluster::Node;
use scylla::routing::Shard;
use scylla::routing::locator;
use scylla_cql::frame::response::result::TableSpec;

pub(crate) mod sharding;
use sharding::Sharder;

use crate::cluster::node;

#[pyclass(frozen)]
#[derive(Clone)]
pub(crate) struct Token {
    pub(crate) _inner: scylla::routing::Token,
}

#[pymethods]
impl Token {
    #[new]
    fn new(value: i64) -> Self {
        Self {
            _inner: scylla::routing::Token::new(value),
        }
    }

    fn value(&self) -> i64 {
        self._inner.value()
    }

    fn __repr__(&self) -> String {
        format!("Token({})", self._inner.value())
    }

    fn __eq__(&self, other: &Token) -> bool {
        self._inner == other._inner
    }

    fn __hash__(&self) -> i64 {
        self._inner.value()
    }
}

#[pyclass(frozen)]
#[derive(Clone)]
pub(crate) struct ReplicaSet {
    replicas: Vec<(Arc<Node>, Shard)>,
}

impl ReplicaSet {
    pub(crate) fn from_rust<'a>(replica_set: scylla::routing::locator::ReplicaSet<'a>) -> Self {
        ReplicaSet {
            replicas: replica_set
                .into_iter()
                .map(|(node_ref, shard)| (Arc::clone(node_ref), shard))
                .collect(),
        }
    }
}

#[pymethods]
impl ReplicaSet {
    fn __len__(&self) -> usize {
        self.replicas.len()
    }

    fn is_empty(&self) -> bool {
        self.replicas.is_empty()
    }

    fn to_list<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        PyList::new(
            py,
            self.replicas.iter().map(|(node, shard)| {
                (
                    node::Node {
                        _inner: Arc::clone(node),
                    },
                    *shard,
                )
            }),
        )
    }

    fn __iter__(slf: PyRef<'_, Self>) -> ReplicaSetIterator {
        ReplicaSetIterator {
            replicas: slf.replicas.clone().into_iter(),
        }
    }

    fn __repr__(&self) -> String {
        format!("ReplicaSet(len={})", self.replicas.len())
    }
}

#[pyclass]
pub(crate) struct ReplicaSetIterator {
    replicas: std::vec::IntoIter<(Arc<Node>, Shard)>,
}

#[pymethods]
impl ReplicaSetIterator {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(&mut self) -> Option<(node::Node, Shard)> {
        self.replicas
            .next()
            .map(|(node, shard)| (node::Node { _inner: node }, shard))
    }
}

#[pyclass(frozen)]
#[derive(Clone)]
pub(crate) struct ReplicaLocator {
    pub(crate) _inner: locator::ReplicaLocator,
}

impl ReplicaLocator {
    pub(crate) fn from_rust(inner: locator::ReplicaLocator) -> Self {
        ReplicaLocator { _inner: inner }
    }
}

#[pymethods]
impl ReplicaLocator {
    fn replicas_for_token(
        &self,
        token: Token,
        strategy: crate::cluster::metadata::Strategy,
        datacenter: Option<&str>,
        keyspace: &str,
        table: &str,
    ) -> ReplicaSet {
        let table_spec = TableSpec::borrowed(keyspace, table);
        let replica_set =
            self._inner
                .replicas_for_token(token._inner, &strategy._inner, datacenter, &table_spec);
        ReplicaSet::from_rust(replica_set)
    }

    fn unique_nodes_in_global_ring<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        PyList::new(
            py,
            self._inner
                .unique_nodes_in_global_ring()
                .iter()
                .map(|n| node::Node {
                    _inner: Arc::clone(n),
                }),
        )
    }

    fn unique_nodes_in_datacenter_ring<'py>(
        &self,
        datacenter: &str,
        py: Python<'py>,
    ) -> PyResult<Option<Bound<'py, PyList>>> {
        self._inner
            .unique_nodes_in_datacenter_ring(datacenter)
            .map(|nodes| {
                PyList::new(
                    py,
                    nodes.iter().map(|n| node::Node {
                        _inner: Arc::clone(n),
                    }),
                )
            })
            .transpose()
    }

    fn datacenter_names<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyList>> {
        PyList::new(py, self._inner.datacenter_names())
    }

    fn ring_len(&self) -> usize {
        self._inner.ring().len()
    }

    fn __repr__(&self) -> String {
        format!(
            "ReplicaLocator(ring_len={}, datacenters={:?})",
            self._inner.ring().len(),
            self._inner.datacenter_names(),
        )
    }
}

#[pymodule]
pub(crate) fn routing(_py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<Sharder>()?;
    module.add_class::<Token>()?;
    module.add_class::<ReplicaSet>()?;
    module.add_class::<ReplicaSetIterator>()?;
    module.add_class::<ReplicaLocator>()?;
    Ok(())
}
