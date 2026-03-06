use std::convert::Infallible;
use std::sync::{Arc, Mutex};

use pyo3::intern;
use pyo3::sync::MutexExt;
use pyo3::types::PyIterator;
use pyo3::{prelude::*, types::PyTuple};
use scylla::{
    frame::response::result::TableSpec,
    policies::load_balancing::{FallbackPlan, LoadBalancingPolicy, RoutingInfo},
    routing::{self, Shard},
    statement,
};

use scylla::cluster::{self, NodeRef};

use crate::cluster::node::NodeShard;
use crate::cluster::state::TableSpecOwned;
use crate::{
    cluster::state::ClusterState,
    enums::{Consistency, SerialConsistency},
    routing::Token,
};

#[pyclass]
struct NodeShardIterator {
    _inner: std::vec::IntoIter<NodeShard>,
}

#[pymethods]
impl NodeShardIterator {
    fn __iter__(slf: PyRef<Self>) -> PyRef<Self> {
        slf
    }

    fn __next__(mut slf: PyRefMut<Self>) -> Option<NodeShard> {
        slf._inner.next()
    }
}

#[pyclass]
#[derive(Clone)]
pub(crate) struct DefaultPolicy {
    pub(crate) preferred_datacenter: Option<String>,
    pub(crate) preferred_datacenter_and_rack: Option<(String, String)>,
    pub(crate) token_aware: bool,
    pub(crate) permit_dc_failover: bool,
    pub(crate) enable_shuffling_replicas: bool,
}

/// Builds an `Arc<dyn LoadBalancingPolicy>` and the corresponding
/// `PyLoadBalancingPolicy` wrapper from a raw `Py<PyAny>`.
///
/// In case of `DefaultPolicy` a native Rust `DefaultPolicy` is constructed.
pub(crate) fn build_load_balancing_policy(
    policy: Py<PyAny>,
) -> (PyLoadBalancingPolicy, Arc<dyn LoadBalancingPolicy>) {
    Python::attach(|py| {
        if let Ok(default_policy) = policy.extract::<DefaultPolicy>(py) {
            let mut lbp_builder = scylla::policies::load_balancing::DefaultPolicy::builder();

            lbp_builder = lbp_builder
                .enable_shuffling_replicas(default_policy.enable_shuffling_replicas)
                .permit_dc_failover(default_policy.permit_dc_failover)
                .token_aware(default_policy.token_aware);

            if let Some(pref_dc) = default_policy.preferred_datacenter {
                lbp_builder = lbp_builder.prefer_datacenter(pref_dc);
            }

            if let Some((pref_dc, pref_rack)) = default_policy.preferred_datacenter_and_rack {
                lbp_builder = lbp_builder.prefer_datacenter_and_rack(pref_dc, pref_rack);
            }

            let stored = PyLoadBalancingPolicy {
                _inner: policy,
                cluster_cache: Mutex::new(None),
            };
            (stored, lbp_builder.build())
        } else {
            // User-defined policy: wrap and dispatch through the trait impl.
            let lbp = PyLoadBalancingPolicy {
                _inner: policy,
                cluster_cache: Mutex::new(None),
            };
            let arc: Arc<dyn LoadBalancingPolicy> = Arc::new(lbp.clone());
            (lbp, arc)
        }
    })
}

#[pymethods]
impl DefaultPolicy {
    #[new]
    #[pyo3(signature = (
        preferred_datacenter = None,
        preferred_datacenter_and_rack = None,
        token_aware = true,
        permit_dc_failover = false,
        enable_shuffling_replicas = true,
    ))]
    fn new(
        preferred_datacenter: Option<String>,
        preferred_datacenter_and_rack: Option<(String, String)>,
        token_aware: bool,
        permit_dc_failover: bool,
        enable_shuffling_replicas: bool,
    ) -> Self {
        Self {
            preferred_datacenter,
            preferred_datacenter_and_rack,
            token_aware,
            permit_dc_failover,
            enable_shuffling_replicas,
        }
    }

    /// This method is defined to satisfy Python's LoadBalancingPolicy Protocol.
    /// In load balancing underlaying Rust DefaultPolicy is used.
    fn pick_targets(
        &self,
        routing_info: RoutingInfoOwned,
        cluster_state: ClusterState,
    ) -> PyResult<NodeShardIterator> {
        let mut lbp_builder = scylla::policies::load_balancing::DefaultPolicy::builder();

        lbp_builder = lbp_builder
            .enable_shuffling_replicas(self.enable_shuffling_replicas)
            .permit_dc_failover(self.permit_dc_failover)
            .token_aware(self.token_aware);

        if let Some(pref_dc) = self.preferred_datacenter.clone() {
            lbp_builder = lbp_builder.prefer_datacenter(pref_dc);
        }

        if let Some((pref_dc, pref_rack)) = self.preferred_datacenter_and_rack.clone() {
            lbp_builder = lbp_builder.prefer_datacenter_and_rack(pref_dc, pref_rack);
        }

        let policy = lbp_builder.build();

        let table_spec = routing_info
            ._table
            .as_ref()
            .map(|(ks, tbl)| TableSpec::borrowed(ks, tbl));

        let request = RoutingInfo {
            consistency: routing_info._consistency,
            serial_consistency: routing_info._serial_consistency,
            token: routing_info._token,
            table: table_spec.as_ref(),
            is_confirmed_lwt: routing_info._is_confirmed_lwt,
        };

        let cluster = &cluster_state._inner;

        let nodes = policy
            .fallback(&request, cluster)
            .map(|(node_ref, shard)| NodeShard {
                _inner: (node_ref.host_id, shard),
            })
            .collect::<Vec<_>>();

        Ok(NodeShardIterator {
            _inner: nodes.into_iter(),
        })
    }
}

pub(crate) struct PyLoadBalancingPolicy {
    pub(crate) _inner: Py<PyAny>,
    pub(crate) cluster_cache: Mutex<Option<(Arc<cluster::ClusterState>, Py<ClusterState>)>>,
}

impl<'py> IntoPyObject<'py> for PyLoadBalancingPolicy {
    type Target = PyAny;
    type Output = Bound<'py, PyAny>;
    type Error = Infallible;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        Ok(self._inner.clone_ref(py).into_bound(py))
    }
}

impl Clone for PyLoadBalancingPolicy {
    fn clone(&self) -> Self {
        PyLoadBalancingPolicy {
            _inner: self._inner.clone(),
            cluster_cache: Mutex::new(None),
        }
    }
}

impl std::fmt::Debug for PyLoadBalancingPolicy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PyLoadBalancingPolicy")
            .field("_inner", &self._inner)
            .finish()
    }
}

struct PyTargetsIter<'a> {
    py_iter: Py<PyIterator>,
    cluster: &'a cluster::ClusterState,
    exhausted: bool,
}

/// Iterator for Python-defined targets.
///
/// Lazily aquires GIL for each `next()`
/// On error logs and exhausts the iterator.
/// This means when first `next()` errors this would log and return empty iterator.
impl<'a> Iterator for PyTargetsIter<'a> {
    type Item = (NodeRef<'a>, Option<Shard>);

    fn next(&mut self) -> Option<Self::Item> {
        if self.exhausted {
            return None;
        }

        Python::attach(|py| -> Option<(NodeRef<'a>, Option<Shard>)> {
            let mut py_iter: Bound<'_, PyIterator> = self.py_iter.bind(py).clone();
            match py_iter.next() {
                None => {
                    self.exhausted = true;
                    None
                }
                Some(Err(err)) => {
                    log::error!("Failed to iterate over 'pick_targets' result: {}", err);
                    self.exhausted = true;
                    None
                }
                Some(Ok(item)) => {
                    let Ok(node_shard) = item.extract::<PyRef<NodeShard>>().map_err(|err| {
                        log::error!(
                            "Failed to extract NodeShard from 'pick_targets' iterator: {}",
                            err
                        );
                    }) else {
                        self.exhausted = true;
                        return None;
                    };
                    if let Some(node) = self.cluster.known_peers.get(&node_shard._inner.0) {
                        Some((node, node_shard._inner.1))
                    } else {
                        log::error!(
                            "Failed to retrieve node with host_id: {}, it's ignored in load balancing",
                            node_shard._inner.0
                        );
                        self.exhausted = true;
                        None
                    }
                }
            }
        })
    }
}

impl LoadBalancingPolicy for PyLoadBalancingPolicy {
    fn pick<'a>(
        &'a self,
        _request: &'a RoutingInfo,
        _cluster: &'a cluster::ClusterState,
    ) -> Option<(NodeRef<'a>, Option<Shard>)> {
        None
    }

    fn fallback<'a>(
        &'a self,
        request: &'a RoutingInfo,
        cluster: &'a cluster::ClusterState,
    ) -> FallbackPlan<'a> {
        let py_iter_result = Python::attach(|py| -> PyResult<Py<PyIterator>> {
            let py_cluster = {
                let mut cluster_cache = self.cluster_cache.lock_py_attached(py).unwrap();

                let incoming_ptr = cluster as *const cluster::ClusterState;
                let is_same = cluster_cache
                    .as_ref()
                    .map(|(cached_arc, _)| Arc::as_ptr(cached_arc) == incoming_ptr)
                    .unwrap_or(false);

                if is_same {
                    cluster_cache
                        .as_ref()
                        .expect("Must be Some")
                        .1
                        .clone_ref(py)
                } else {
                    // SAFETY:
                    // &'a cluster::ClusterState comes from `Arc::deref`, so it's always in an Arc.
                    // Claiming exactly 1 strong reference let's us "clone" the Arc through the raw pointer.
                    // This let's us invalidate cache entries when pointers don't match.
                    // This approach is sound due to PyLoadBalancingPolicy keeping the ClusterState alive
                    // thus preventing ABA problem and guaranteeing that:
                    // ClusterState changes if and only if the pointer changes.
                    let new_arc = unsafe {
                        Arc::increment_strong_count(incoming_ptr);
                        Arc::from_raw(incoming_ptr)
                    };
                    let new_py = Py::new(
                        py,
                        ClusterState {
                            _inner: cluster.clone(),
                        },
                    )
                    .expect("Should always be able to create a pointer");

                    let result = new_py.clone_ref(py);
                    *cluster_cache = Some((new_arc, new_py));
                    result
                }
            }; // lock
            let python_request = RoutingInfoOwned::to_python(request);

            let python_pick_targets = self
                ._inner
                .call_method1(
                    py,
                    intern!(py, "pick_targets"),
                    (python_request, py_cluster),
                )
                .map_err(|err| {
                    log::error!(
                        "Failed to call 'pick_targets' method on LoadBalancing Policy: {}",
                        err
                    );
                    err
                })?;

            let py_iter = python_pick_targets
                .extract::<Py<PyIterator>>(py)
                .map_err(|err| {
                    log::error!(
                        "Failed to call 'pick_targets' method on LoadBalancing Policy: {}",
                        err
                    );
                    err
                })?;
            Ok(py_iter)
        });

        let Ok(py_iter) = py_iter_result else {
            // On error log and return empty plan
            return Box::new(std::iter::empty());
        };

        Box::new(PyTargetsIter {
            py_iter,
            cluster,
            exhausted: false,
        })
    }

    fn name(&self) -> String {
        let name = Python::attach(|py| {
            self._inner
                .bind(py)
                .get_type()
                .name()
                .expect("Type name shouldn't error")
                .to_string()
        });
        format!("LoadbalancingPolicy for {}", name)
    }
}

#[pyclass(name = "RoutingInfo")]
#[derive(Debug, Clone)]
pub(crate) struct RoutingInfoOwned {
    pub(crate) _consistency: statement::Consistency,
    pub(crate) _serial_consistency: Option<statement::SerialConsistency>,
    pub(crate) _token: Option<routing::Token>,
    pub(crate) _table: Option<TableSpecOwned>,
    pub(crate) _is_confirmed_lwt: bool,
}

#[pymethods]
impl RoutingInfoOwned {
    #[getter]
    fn consistency(&self) -> Consistency {
        Consistency::to_python(self._consistency)
    }

    #[getter]
    fn serial_consistency(&self) -> Option<SerialConsistency> {
        self._serial_consistency.map(SerialConsistency::to_python)
    }

    #[getter]
    fn token(&self) -> Option<Token> {
        self._token.map(|t| Token { _inner: t })
    }

    #[getter]
    fn table<'py>(&self, py: Python<'py>) -> PyResult<Option<Bound<'py, PyTuple>>> {
        match self._table.as_ref() {
            Some((ks, tbl)) => Ok(Some(PyTuple::new(py, [ks, tbl])?)),
            None => Ok(None),
        }
    }

    #[getter]
    fn is_confirmed_lwt(&self) -> bool {
        self._is_confirmed_lwt
    }
}

impl RoutingInfoOwned {
    pub(crate) fn to_python(routing_info: &'_ RoutingInfo) -> Self {
        RoutingInfoOwned {
            _consistency: routing_info.consistency,
            _serial_consistency: routing_info.serial_consistency,
            _token: routing_info.token,
            _table: routing_info
                .table
                .map(|t| (t.ks_name().to_string(), t.table_name().to_string())),
            _is_confirmed_lwt: routing_info.is_confirmed_lwt,
        }
    }
}

#[pymodule]
pub(crate) fn load_balancing(_py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<RoutingInfoOwned>()?;
    module.add_class::<DefaultPolicy>()?;
    Ok(())
}
