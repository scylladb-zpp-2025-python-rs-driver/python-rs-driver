use std::convert::Infallible;
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use pyo3::exceptions::PyValueError;
use pyo3::{prelude::*, types::PyTuple};
use scylla::{
    frame::response::result::TableSpec,
    policies::{
        self,
        load_balancing::{FallbackPlan, LatencyAwarenessBuilder, LoadBalancingPolicy, RoutingInfo},
    },
    routing::{self, Shard},
    statement,
};
use uuid::Uuid;

use scylla::cluster::{self, NodeRef};

use crate::{
    cluster::state::{ClusterState, NodeShard},
    enums::{Consistency, SerialConsistency},
    routing::Token,
};

pub(crate) type TableSpecOwned = (String, String);

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

#[pyclass(frozen)]
#[derive(Clone)]
pub(crate) struct LatencyAwareness {
    exclusion_threshold: f64,
    retry_period: Duration,
    update_rate: Duration,
    minimum_measurements: usize,
    scale: Duration,
}

#[pymethods]
impl LatencyAwareness {
    #[new]
    #[pyo3(signature = (
        exclusion_threshold = 2.0,
        retry_period_secs = 10.0,
        update_rate_secs = 0.1,
        minimum_measurements = 50,
        scale_secs = 0.1,
    ))]
    fn new(
        exclusion_threshold: f64,
        retry_period_secs: f64,
        update_rate_secs: f64,
        minimum_measurements: usize,
        scale_secs: f64,
    ) -> PyResult<Self> {
        if !exclusion_threshold.is_finite() || exclusion_threshold <= 0.0 {
            return Err(PyErr::new::<PyValueError, _>(
                "exclusion_threshold must be a positive, finite number",
            ));
        }
        let slf = Self {
            exclusion_threshold,
            retry_period: Duration::try_from_secs_f64(retry_period_secs).map_err(|_| {
                PyErr::new::<PyValueError, _>("retry_period_secs must be a positive, finite number")
            })?,
            update_rate: Duration::try_from_secs_f64(update_rate_secs).map_err(|_| {
                PyErr::new::<PyValueError, _>("update_rate_secs must be a positive, finite number")
            })?,
            minimum_measurements,
            scale: Duration::try_from_secs_f64(scale_secs).map_err(|_| {
                PyErr::new::<PyValueError, _>("scale_secs must be a positive, finite number")
            })?,
        };
        Ok(slf)
    }
}

impl LatencyAwareness {
    fn build(&self) -> LatencyAwarenessBuilder {
        LatencyAwarenessBuilder::new()
            .exclusion_threshold(self.exclusion_threshold)
            .retry_period(self.retry_period)
            .update_rate(self.update_rate)
            .minimum_measurements(self.minimum_measurements)
            .scale(self.scale)
    }
}

#[pyclass]
pub(crate) struct DefaultPolicy {
    _inner: OnceLock<Arc<dyn LoadBalancingPolicy>>,
    preferred_datacenter: Option<String>,
    preferred_datacenter_and_rack: Option<(String, String)>,
    token_aware: Option<bool>,
    permit_dc_failover: Option<bool>,
    latency_awareness: Option<LatencyAwareness>,
    enable_shuffling_replicas: Option<bool>,
}

impl DefaultPolicy {
    fn get_inner(&self) -> &Arc<dyn LoadBalancingPolicy> {
        self._inner.get_or_init(|| {
            let mut builder = policies::load_balancing::DefaultPolicy::builder();

            if let Some(ref dc) = self.preferred_datacenter {
                builder = builder.prefer_datacenter(dc.clone());
            }

            if let Some((ref dc, ref rack)) = self.preferred_datacenter_and_rack {
                builder = builder.prefer_datacenter_and_rack(dc.clone(), rack.clone());
            }

            if let Some(token_aware) = self.token_aware {
                builder = builder.token_aware(token_aware);
            }

            if let Some(permit_dc_failover) = self.permit_dc_failover {
                builder = builder.permit_dc_failover(permit_dc_failover);
            }

            if let Some(ref la) = self.latency_awareness {
                builder = builder.latency_awareness(la.build());
            }

            if let Some(enable_shuffling_replicas) = self.enable_shuffling_replicas {
                builder = builder.enable_shuffling_replicas(enable_shuffling_replicas);
            }

            builder.build()
        })
    }
}

#[pymethods]
impl DefaultPolicy {
    #[new]
    #[pyo3(signature = (
        preferred_datacenter = None,
        preferred_datacenter_and_rack = None,
        token_aware = None,
        permit_dc_failover = None,
        latency_awareness = None,
        enable_shuffling_replicas = None,
    ))]
    fn new(
        preferred_datacenter: Option<String>,
        preferred_datacenter_and_rack: Option<(String, String)>,
        token_aware: Option<bool>,
        permit_dc_failover: Option<bool>,
        latency_awareness: Option<LatencyAwareness>,
        enable_shuffling_replicas: Option<bool>,
    ) -> Self {
        Self {
            _inner: OnceLock::new(),
            preferred_datacenter,
            preferred_datacenter_and_rack,
            token_aware,
            permit_dc_failover,
            latency_awareness,
            enable_shuffling_replicas,
        }
    }

    fn fallback(
        &self,
        routing_info: RoutingInfoOwned,
        cluster_state: ClusterState,
    ) -> PyResult<NodeShardIterator> {
        let ts = routing_info
            ._table
            .map(|table| TableSpec::owned(table.0, table.1));

        let ri = RoutingInfo {
            consistency: routing_info._consistency,
            serial_consistency: routing_info._serial_consistency,
            token: routing_info._token,
            table: ts.as_ref(),
            is_confirmed_lwt: routing_info._is_confirmed_lwt,
        };

        let vec = self
            .get_inner()
            .fallback(&ri, &cluster_state._inner)
            .map(|item| NodeShard {
                _inner: (item.0.host_id, item.1),
            })
            .collect::<Vec<_>>();

        Ok(NodeShardIterator {
            _inner: vec.into_iter(),
        })
    }
}

#[derive(Debug)]
#[allow(dead_code)]
pub(crate) struct PyLoadBalancingPolicy {
    pub(crate) _inner: Py<PyAny>,
}

impl Clone for PyLoadBalancingPolicy {
    fn clone(&self) -> Self {
        PyLoadBalancingPolicy {
            _inner: Python::attach(|py| self._inner.clone_ref(py)),
        }
    }
}

impl<'py> IntoPyObject<'py> for PyLoadBalancingPolicy {
    type Target = PyAny;
    type Output = Bound<'py, PyAny>;
    type Error = Infallible;

    fn into_pyobject(self, py: Python<'py>) -> Result<Self::Output, Self::Error> {
        Ok(self._inner.into_bound(py))
    }
}

impl PyLoadBalancingPolicy {
    #[allow(dead_code)]
    fn extract_node_shards(
        py: Python,
        python_fallback: &Bound<'_, PyAny>,
    ) -> PyResult<Vec<(Uuid, Option<Shard>)>> {
        let iter = python_fallback.try_iter().map_err(|err| {
            log::error!(
                "Provided 'fallback' function doesn't return an iterator: {}",
                err
            );
            err
        })?;

        iter.map(|item_result| {
            let item = item_result.map_err(|err| {
                log::error!("Failed to iterate over 'fallback' result: {}", err);
                err
            })?;

            let node_shard = item.unbind().extract::<Py<NodeShard>>(py).map_err(|err| {
                log::error!(
                    "Failed to extract NodeShard from 'fallback' iterator: {}",
                    err
                );
                err
            })?;

            Ok(node_shard.get()._inner)
        })
        .collect()
    }
}

impl LoadBalancingPolicy for PyLoadBalancingPolicy {
    fn pick<'a>(
        &'a self,
        request: &'a RoutingInfo,
        cluster: &'a cluster::ClusterState,
    ) -> Option<(NodeRef<'a>, Option<Shard>)> {
        self.fallback(request, cluster).next()
    }

    fn fallback<'a>(
        &'a self,
        request: &'a RoutingInfo,
        cluster: &'a cluster::ClusterState,
    ) -> FallbackPlan<'a> {
        let python_request = RoutingInfoOwned::to_python(request);
        let python_cluster = ClusterState {
            _inner: cluster.clone(),
        };

        let fallback_result = Python::attach(|py| -> PyResult<Vec<(Uuid, Option<Shard>)>> {
            let python_fallback = self
                ._inner
                .call_method1(py, "fallback", (python_request, python_cluster))
                .map_err(|err| {
                    log::error!(
                        "Failed to call 'fallback' method on LoadBalancing Policy: {}",
                        err
                    );
                    err
                })?;

            Self::extract_node_shards(py, python_fallback.bind(py))
        });

        let Ok(node_shards) = fallback_result else {
            return Box::new(std::iter::empty());
        };

        Box::new(node_shards.into_iter().filter_map(move |(host_id, shard)| {
            cluster.known_peers.get(&host_id).map_or_else(
                || {
                    log::error!(
                        "Failed to retrieve node with host_id: {}, it's ignored in loadbalancing",
                        host_id
                    );
                    None
                },
                |node| Some((node, shard)),
            )
        }))
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
#[derive(Clone)]
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
    #[allow(dead_code)]
    pub(crate) fn to_python(routing_info: &RoutingInfo) -> Self {
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
    module.add_class::<LatencyAwareness>()?;
    module.add_class::<RoutingInfoOwned>()?;
    module.add_class::<DefaultPolicy>()?;
    Ok(())
}
