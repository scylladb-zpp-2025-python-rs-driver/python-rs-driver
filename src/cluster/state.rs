use std::{collections::HashMap, sync::Arc};

use pyo3::{exceptions::PyRuntimeError, prelude::*, types::PyString};
use scylla::{
    cluster::{self},
    routing::Shard,
};
use uuid::Uuid;

use crate::{
    cluster::{metadata::Keyspace, node::Node},
    policies::load_balancing::TableSpecOwned,
    routing::Token,
};

#[pyclass(frozen)]
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

#[pyclass]
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

    fn keyspaces_iter(&self) -> HashMap<String, Keyspace> {
        self._inner
            .keyspaces_iter()
            .map(|(name, ks)| (name.to_string(), Keyspace { _inner: ks.clone() }))
            .collect::<HashMap<String, Keyspace>>()
    }

    fn get_nodes_info(&self) -> Vec<Node> {
        self._inner
            .get_nodes_info()
            .iter()
            .map(|node| Node {
                _inner: Arc::clone(node),
            })
            .collect()
    }

    fn compute_token(
        &self,
        keyspace: String,
        table: String,
        partition_key: i32,
    ) -> PyResult<Token> {
        self._inner
            .compute_token(keyspace.as_str(), table.as_str(), &(partition_key,))
            .map(|t| Ok(Token { _inner: t }))
            .map_err(|e| PyErr::new::<PyRuntimeError, _>(format!("Error computing token: {}", e)))?
    }

    fn get_token_endpoints(&self, table_spec: TableSpecOwned, token: Token) -> Vec<(Node, Shard)> {
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
            })
            .collect()
    }

    fn get_endpoints(&self, table_spec: TableSpecOwned, partition_key: i32) -> PyResult<Vec<Node>> {
        let endpoints = self
            ._inner
            .get_endpoints(&table_spec.0, &table_spec.1, &(partition_key,))
            .map_err(|e| {
                PyErr::new::<PyRuntimeError, _>(format!("Error getting endpoints: {}", e))
            })?
            .iter()
            .map(|node| Node {
                _inner: Arc::clone(&node.0),
            })
            .collect();
        Ok(endpoints)
    }

    fn datacenter_names<'py>(&self, py: Python<'py>) -> Vec<Bound<'py, PyString>> {
        let rust_dc_names = self._inner.replica_locator().datacenter_names();
        rust_dc_names
            .iter()
            .map(|dc_name| PyString::new(py, dc_name))
            .collect()
    }

    fn unique_nodes_in_global_ring(&self) -> Vec<Node> {
        self._inner
            .replica_locator()
            .unique_nodes_in_global_ring()
            .iter()
            .map(|node| Node {
                _inner: Arc::clone(node),
            })
            .collect()
    }

    fn unique_nodes_in_datacenter_ring(&self, datacenter: &str) -> Option<Vec<Node>> {
        self._inner
            .replica_locator()
            .unique_nodes_in_datacenter_ring(datacenter)
            .map(|nodes| {
                nodes
                    .iter()
                    .map(|node| Node {
                        _inner: Arc::clone(node),
                    })
                    .collect()
            })
    }

    fn ring_len(&self) -> usize {
        self._inner.replica_locator().ring().len()
    }

    fn __repr__(&self) -> String {
        format!(
            "ClusterState(nodes={}, keyspaces={})",
            self._inner.get_nodes_info().len(),
            self._inner.keyspaces_iter().count()
        )
    }
}
