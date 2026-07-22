use crate::cluster::node::PyNode;
use crate::cluster::state::PyClusterState;
use crate::enums::PyConsistency;
use crate::enums::PySerialConsistency;
use crate::errors::DriverLoadBalancingPolicyError;
use crate::routing::PyToken;
use pyo3::PyAny;
use pyo3::intern;
use pyo3::prelude::*;
use pyo3::prelude::{PyAnyMethods, PyModule, PyModuleMethods};
use pyo3::sync::MutexExt;
use pyo3::sync::PyOnceLock;
use pyo3::types::PyIterator;
use pyo3::types::PyList;
use pyo3::types::PyString;
use pyo3::{Bound, BoundObject, Py, PyResult, Python, pyclass, pymethods, pymodule};
use scylla::cluster::ClusterState;
use scylla::cluster::NodeRef;
use scylla::frame::response::result::TableSpec;
use scylla::policies::load_balancing::DefaultPolicy;
use scylla::policies::load_balancing::FallbackPlan;
use scylla::policies::load_balancing::LoadBalancingPolicy;
use scylla::policies::load_balancing::RoutingInfo;
use scylla::routing::NodeLocationPreference;
use scylla::routing::Shard;
use scylla::routing::Token;
use scylla::statement::{Consistency, SerialConsistency};
use std::fmt::Debug;
use std::sync::Arc;
use std::sync::Mutex;

/// Python representation of routing information for a request.
/// Exposed to Python as `RoutingInfo`.
#[pyclass(frozen, name = "RoutingInfo")]
pub struct PyRoutingInfo {
    consistency: Consistency,
    serial_consistency: Option<SerialConsistency>,
    token: Option<Token>,
    ks_name: Option<String>,
    table_name: Option<String>,
    #[pyo3(get)]
    is_confirmed_lwt: bool,
    node_location_preference: NodeLocationPreference,

    // Cached Python-side representations used by the getters.
    py_consistency: PyOnceLock<Py<PyConsistency>>,
    py_serial_consistency: PyOnceLock<Py<PySerialConsistency>>,
    py_token: PyOnceLock<Py<PyToken>>,
    py_ks: PyOnceLock<Py<PyString>>,
    py_table: PyOnceLock<Py<PyString>>,
    py_preferred_datacenter: PyOnceLock<Py<PyString>>,
    py_preferred_rack: PyOnceLock<Py<PyString>>,
}

impl<'a> From<&RoutingInfo<'a>> for PyRoutingInfo {
    fn from(info: &RoutingInfo<'a>) -> Self {
        let ks_name = info.table.map(|t| t.ks_name().to_string());
        let table_name = info.table.map(|t| t.table_name().to_string());

        Self {
            consistency: info.consistency,
            serial_consistency: info.serial_consistency,
            token: info.token,
            ks_name,
            table_name,
            is_confirmed_lwt: info.is_confirmed_lwt,
            node_location_preference: info.node_location_preference.clone(),

            py_consistency: PyOnceLock::new(),
            py_serial_consistency: PyOnceLock::new(),
            py_token: PyOnceLock::new(),
            py_ks: PyOnceLock::new(),
            py_table: PyOnceLock::new(),
            py_preferred_datacenter: PyOnceLock::new(),
            py_preferred_rack: PyOnceLock::new(),
        }
    }
}

impl PyRoutingInfo {
    fn to_routing_info<'a>(&'a self, table_spec: Option<&'a TableSpec<'a>>) -> RoutingInfo<'a> {
        RoutingInfo::new(
            self.consistency,
            self.serial_consistency,
            self.token,
            table_spec,
            self.is_confirmed_lwt,
            &self.node_location_preference,
        )
    }
}

#[pymethods]
impl PyRoutingInfo {
    #[getter]
    fn consistency(&self, py: Python<'_>) -> PyResult<Py<PyConsistency>> {
        let bound_enum = self.py_consistency.get_or_try_init(py, || {
            let py_enum: PyConsistency = self.consistency.into();
            Py::new(py, py_enum)
        })?;
        Ok(bound_enum.clone_ref(py))
    }

    #[getter]
    fn serial_consistency(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        match self.serial_consistency {
            None => Ok(py.None()),
            Some(sc) => {
                let bound_enum = self.py_serial_consistency.get_or_try_init(py, || {
                    let py_enum: PySerialConsistency = sc.into();
                    Py::new(py, py_enum)
                })?;
                Ok(bound_enum.clone_ref(py).into_any())
            }
        }
    }

    #[getter]
    fn token(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        match self.token {
            None => Ok(py.None()),
            Some(token) => {
                let bound_token = self
                    .py_token
                    .get_or_try_init(py, || Py::new(py, PyToken::from(token)))?;
                Ok(bound_token.clone_ref(py).into_any())
            }
        }
    }

    #[getter]
    fn keyspace(&self, py: Python<'_>) -> Py<PyAny> {
        match &self.ks_name {
            None => py.None(),
            Some(ks) => {
                let bound_str = self
                    .py_ks
                    .get_or_init(py, || PyString::new(py, ks).unbind());
                bound_str.clone_ref(py).into_any()
            }
        }
    }

    #[getter]
    fn table(&self, py: Python<'_>) -> Py<PyAny> {
        match &self.table_name {
            None => py.None(),
            Some(table) => {
                let bound_str = self
                    .py_table
                    .get_or_init(py, || PyString::new(py, table).unbind());
                bound_str.clone_ref(py).into_any()
            }
        }
    }

    #[getter]
    fn preferred_datacenter(&self, py: Python<'_>) -> Py<PyAny> {
        match self.node_location_preference.datacenter() {
            None => py.None(),
            Some(dc) => {
                let bound_str = self
                    .py_preferred_datacenter
                    .get_or_init(py, || PyString::new(py, dc).unbind());
                bound_str.clone_ref(py).into_any()
            }
        }
    }

    #[getter]
    fn preferred_rack(&self, py: Python<'_>) -> Py<PyAny> {
        match self.node_location_preference.rack() {
            None => py.None(),
            Some(rack) => {
                let bound_str = self
                    .py_preferred_rack
                    .get_or_init(py, || PyString::new(py, rack).unbind());
                bound_str.clone_ref(py).into_any()
            }
        }
    }

    fn __repr__(&self, py: Python<'_>) -> PyResult<Py<PyString>> {
        let repr_str = PyString::from_fmt(
            py,
            format_args!(
                "RoutingInfo(consistency={:?}, serial_consistency={:?}, token={}, keyspace={:?}, table={:?}, is_confirmed_lwt={})",
                self.consistency,
                self.serial_consistency,
                self.token(py)?.bind(py).repr()?,
                self.ks_name,
                self.table_name,
                self.is_confirmed_lwt
            ),
        )?;

        Ok(repr_str.into())
    }
}

/// Represents a custom load balancing policy object implemented by the Python user.
#[derive(Debug)]
struct CustomLoadBalancingPolicy {
    inner: Py<PyAny>,
    cluster_cache: Mutex<Option<(Arc<ClusterState>, Py<PyClusterState>)>>,
}

impl CustomLoadBalancingPolicy {
    fn get_cluster_state(
        &self,
        py: Python,
        cluster: &ClusterState,
    ) -> Result<Py<PyClusterState>, PyErr> {
        let incoming_ptr = cluster as *const ClusterState;
        let mut cache = self.cluster_cache.lock_py_attached(py).unwrap();

        if let Some((cached_arc, py_cache)) = &*cache
            && std::ptr::eq(Arc::as_ptr(cached_arc), incoming_ptr)
        {
            return Ok(py_cache.clone_ref(py));
        };

        // SAFETY:, HACK:
        // &'a cluster::ClusterState comes from `Arc::deref`, so it's always in an Arc.
        // Claiming exactly 1 strong reference lets us "clone" the Arc through the raw pointer.
        // This lets us invalidate cache entries when pointers don't match.
        // This approach is sound due to PyLoadBalancingPolicy keeping the ClusterState alive
        // thus preventing ABA problem and guaranteeing that:
        // ClusterState changes if and only if the pointer changes.
        let new_arc = unsafe {
            Arc::increment_strong_count(incoming_ptr);
            Arc::from_raw(incoming_ptr)
        };

        let new_py_cluster_state = Py::new(py, PyClusterState::try_from(Arc::clone(&new_arc))?)?;
        *cache = Some((new_arc, new_py_cluster_state.clone_ref(py)));

        Ok(new_py_cluster_state)
    }
}

impl LoadBalancingPolicy for CustomLoadBalancingPolicy {
    fn pick<'a>(
        &'a self,
        _request: &'a RoutingInfo,
        _cluster: &'a ClusterState,
    ) -> Option<(NodeRef<'a>, Option<Shard>)> {
        None
    }

    fn fallback<'a>(
        &'a self,
        request: &'a RoutingInfo,
        cluster: &'a ClusterState,
    ) -> FallbackPlan<'a> {
        let py_result = Python::attach(|py| -> PyResult<Py<PyIterator>> {
            let py_request = PyRoutingInfo::from(request);

            let py_cluster_state = self
                .get_cluster_state(py, cluster)
                .inspect_err(|err| log::error!("Error occurred on Python side: {}", err))?;

            let py_policy = self.inner.bind(py);

            let targets_iterable = py_policy
                .call_method1(intern!(py, "pick_targets"), (py_request, py_cluster_state))
                .inspect_err(|err| {
                    log::error!(
                        "Failed to call 'pick_targets' method on LoadBalancing Policy: {}",
                        err
                    );
                })?;

            let targets_iter = targets_iterable.try_iter().inspect_err(|err| {
                log::error!(
                    "The value returned by 'pick_targets' is not iterable: {}",
                    err
                );
            })?;

            Ok(targets_iter.unbind())
        });

        match py_result {
            Ok(py_iter) => Box::new(PyTargetsIter {
                py_iter,
                exhausted: false,
                cluster,
            }),
            Err(_) => {
                let empty_iter = std::iter::empty::<(NodeRef<'a>, Option<Shard>)>();
                Box::new(empty_iter)
            }
        }
    }

    fn name(&self) -> String {
        Python::attach(|py| {
            self.inner
                .bind(py)
                .get_type()
                .name()
                .map(|name| name.to_string())
                .unwrap_or_else(|_| "Unknown Load Balancing Policy".to_string())
        })
    }
}

struct PyTargetsIter<'a> {
    py_iter: Py<PyIterator>,
    exhausted: bool,
    cluster: &'a ClusterState,
}

/// Lazily acquires the GIL for each `next()`.
/// On error, logs and exhausts the iterator.
/// This means the first `next()` error will log and then the plan becomes empty.
impl<'a> Iterator for PyTargetsIter<'a> {
    type Item = (NodeRef<'a>, Option<Shard>);

    fn next(&mut self) -> Option<Self::Item> {
        if self.exhausted {
            return None;
        }

        Python::attach(|py| -> Option<(NodeRef<'a>, Option<Shard>)> {
            let mut py_iter = self.py_iter.bind(py).clone();

            let Some(item) = py_iter.next() else {
                self.exhausted = true;
                return None;
            };

            let Ok(item) = item.inspect_err(|err| {
                log::error!("Failed to iterate over 'pick_targets' result: {}", err);
            }) else {
                self.exhausted = true;
                return None;
            };

            let Ok((py_node, shard)) =
                item.extract::<(Py<PyNode>, Option<Shard>)>()
                    .inspect_err(|err| {
                        log::error!(
                            "Failed to extract NodeShard from 'pick_targets' iterator: {}",
                            err
                        );
                    })
            else {
                self.exhausted = true;
                return None;
            };

            let id = py_node.get()._inner.host_id;
            let Some(node) = self.cluster.get_node_by_host_id(id) else {
                log::error!(
                    "Failed to retrieve node with host_id: {}, stopping iteration",
                    id
                );
                self.exhausted = true;
                return None;
            };

            Some((node, shard))
        })
    }
}

/// Built-in load balancing policy, equivalent to the Rust driver's DefaultPolicy.
#[derive(Debug)]
#[pyclass(name = "DefaultPolicy", frozen)]
struct PyDefaultPolicy {
    inner: Arc<dyn LoadBalancingPolicy>,
    #[pyo3(get)]
    token_aware: bool,
    #[pyo3(get)]
    permit_dc_failover: bool,
    #[pyo3(get)]
    enable_shuffling_replicas: bool,
    #[pyo3(get)]
    preferred_datacenter: Option<Py<PyString>>,
    #[pyo3(get)]
    preferred_rack: Option<Py<PyString>>,
}

#[pymethods]
impl PyDefaultPolicy {
    #[new]
    #[pyo3(signature = (*,
        preferred_datacenter = None,
        preferred_datacenter_and_rack = None,
        token_aware = true,
        permit_dc_failover = false,
        enable_shuffling_replicas = true,
    ))]
    fn new(
        py: Python<'_>,
        preferred_datacenter: Option<Py<PyString>>,
        preferred_datacenter_and_rack: Option<(Py<PyString>, Py<PyString>)>,
        token_aware: bool,
        permit_dc_failover: bool,
        enable_shuffling_replicas: bool,
    ) -> Result<Self, DriverLoadBalancingPolicyError> {
        let mut builder = DefaultPolicy::builder();

        builder = builder
            .enable_shuffling_replicas(enable_shuffling_replicas)
            .permit_dc_failover(permit_dc_failover)
            .token_aware(token_aware);

        let (preferred_datacenter, preferred_rack) =
            match (preferred_datacenter, preferred_datacenter_and_rack) {
                (_, Some((pref_dc, pref_rack))) => {
                    let dc_str = pref_dc
                        .bind(py)
                        .to_str()
                        .map_err(
                            DriverLoadBalancingPolicyError::default_policy_string_conversion_failed,
                        )?
                        .to_string();

                    let rack_str = pref_rack
                        .bind(py)
                        .to_str()
                        .map_err(
                            DriverLoadBalancingPolicyError::default_policy_string_conversion_failed,
                        )?
                        .to_string();

                    builder = builder.prefer_datacenter_and_rack(dc_str, rack_str);
                    (Some(pref_dc), Some(pref_rack))
                }
                (Some(pref_dc), None) => {
                    let dc_str = pref_dc
                        .bind(py)
                        .to_str()
                        .map_err(
                            DriverLoadBalancingPolicyError::default_policy_string_conversion_failed,
                        )?
                        .to_string();

                    builder = builder.prefer_datacenter(dc_str);
                    (Some(pref_dc), None)
                }
                (None, None) => (None, None),
            };

        Ok(Self {
            inner: builder.build(),
            preferred_datacenter,
            preferred_rack,
            token_aware,
            permit_dc_failover,
            enable_shuffling_replicas,
        })
    }

    fn pick_targets(
        &self,
        py: Python,
        py_routing_info: Py<PyRoutingInfo>,
        py_cluster_state: Py<PyClusterState>,
    ) -> PyResult<Py<PyList>> {
        let py_routing_info = py_routing_info.get();
        let py_cluster_state = py_cluster_state.get();

        let local_spec = match (&py_routing_info.ks_name, &py_routing_info.table_name) {
            (Some(ks), Some(table)) => Some(TableSpec::borrowed(ks.as_str(), table.as_str())),
            _ => None,
        };

        let routing_info = py_routing_info.to_routing_info(local_spec.as_ref());
        let cluster_state = py_cluster_state._inner.as_ref();

        let fallback = self.inner.fallback(&routing_info, cluster_state);

        let list = PyList::empty(py);
        let known_nodes = py_cluster_state.known_nodes.bind(py);

        for (node, shard) in fallback {
            let py_node = known_nodes
                .get_item(node.host_id)?
                .expect("node can't be known by Rust Driver and simultaneously None");
            list.append((py_node, shard))?;
        }

        Ok(list.unbind())
    }
}

#[pymodule]
pub(crate) fn load_balancing(_py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<PyDefaultPolicy>()?;
    module.add_class::<PyRoutingInfo>()?;
    Ok(())
}
