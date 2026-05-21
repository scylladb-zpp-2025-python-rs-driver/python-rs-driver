use crate::RUNTIME;
use crate::errors::{DriverSessionConfigError, DriverSessionConnectionError};
use crate::execution_profile::ExecutionProfile;
use crate::policies::{
    AddressTranslatorInput, AuthenticatorProviderInput, HostFilterInput, TimestampGeneratorInput,
};
use crate::session::Session;
use pyo3::prelude::*;
use pyo3::types::PySequence;
use scylla::authentication::PlainTextAuthenticator;
use scylla::client::session::SessionConfig;
use std::net::{IpAddr, SocketAddr, ToSocketAddrs};
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

#[pyclass]
struct SessionBuilder {
    config: SessionConfig,
}

#[pymethods]
impl SessionBuilder {
    #[new]
    fn new() -> Self {
        Self {
            config: SessionConfig::new(),
        }
    }
    fn contact_points<'py>(
        mut slf: PyRefMut<'py, Self>,
        contact_points: NodeAddrs,
    ) -> PyRefMut<'py, Self> {
        contact_points.add_known_nodes(&mut slf.config);
        slf
    }

    fn execution_profile<'py>(
        mut slf: PyRefMut<'py, Self>,
        execution_profile: ExecutionProfile,
    ) -> PyRefMut<'py, Self> {
        slf.config.default_execution_profile_handle = execution_profile._inner.into_handle();
        slf
    }

    fn user<'py>(
        mut slf: PyRefMut<'py, Self>,
        username: String,
        password: String,
    ) -> PyRefMut<'py, Self> {
        slf.config.authenticator = Some(Arc::new(PlainTextAuthenticator::new(username, password)));
        slf
    }

    fn authenticator_provider<'py>(
        mut slf: PyRefMut<'py, Self>,
        authenticator: AuthenticatorProviderInput,
    ) -> PyRefMut<'py, Self> {
        slf.config.authenticator = Some(authenticator.into_inner());

        slf
    }

    fn address_translator<'py>(
        mut slf: PyRefMut<'py, Self>,
        translator: AddressTranslatorInput,
    ) -> PyRefMut<'py, Self> {
        slf.config.address_translator = Some(translator.into_inner());

        slf
    }

    fn timestamp_generator<'py>(
        mut slf: PyRefMut<'py, Self>,
        generator: TimestampGeneratorInput,
    ) -> PyRefMut<'py, Self> {
        slf.config.timestamp_generator = Some(generator.into_inner());

        slf
    }
    fn host_filter<'py>(
        mut slf: PyRefMut<'py, Self>,
        host_filter: HostFilterInput,
    ) -> PyRefMut<'py, Self> {
        slf.config.host_filter = Some(host_filter.into_inner());

        slf
    }

    async fn connect(&self) -> Result<Session, DriverSessionConnectionError> {
        let config = self.config.clone();
        let session_result = RUNTIME
            .spawn(async move { scylla::client::session::Session::connect(config).await })
            .await?;
        match session_result {
            Ok(session) => Ok(Session {
                _inner: Arc::new(session),
            }),
            Err(err) => Err(DriverSessionConnectionError::new_session_error(err)),
        }
    }
}

pub(crate) enum NodeAddr {
    Host(String),
    SocketAddr(SocketAddr),
}

impl NodeAddr {
    fn add_known_node(self, config: &mut SessionConfig) {
        match self {
            NodeAddr::Host(host) => config.add_known_node(host),
            NodeAddr::SocketAddr(addr) => config.add_known_node_addr(addr),
        }
    }
}

impl ToSocketAddrs for NodeAddr {
    type Iter = std::vec::IntoIter<SocketAddr>;

    fn to_socket_addrs(&self) -> std::io::Result<Self::Iter> {
        match self {
            NodeAddr::SocketAddr(addr) => Ok(vec![*addr].into_iter()),
            NodeAddr::Host(host) => host.to_socket_addrs(),
        }
    }
}

impl<'py> FromPyObject<'_, 'py> for NodeAddr {
    type Error = DriverSessionConfigError;

    fn extract(obj: Borrowed<'_, 'py, PyAny>) -> Result<Self, Self::Error> {
        if let Ok(s) = obj.extract::<String>() {
            return Ok(NodeAddr::Host(s));
        }

        if let Ok((host_str, port)) = obj.extract::<(&str, u16)>() {
            return if let Ok(ip) = IpAddr::from_str(host_str) {
                Ok(NodeAddr::SocketAddr(SocketAddr::new(ip, port)))
            } else {
                Ok(NodeAddr::Host(format!("{}:{}", host_str, port)))
            };
        }

        if let Ok((host, port)) = obj.extract::<(IpAddr, u16)>() {
            return Ok(NodeAddr::SocketAddr(SocketAddr::new(host, port)));
        }

        Err(DriverSessionConfigError::address_type_error(obj))
    }
}

impl TryFrom<NodeAddr> for SocketAddr {
    type Error = DriverSessionConfigError;

    fn try_from(value: NodeAddr) -> Result<Self, Self::Error> {
        match value {
            NodeAddr::SocketAddr(addr) => Ok(addr),
            NodeAddr::Host(addr_str) => SocketAddr::from_str(&addr_str)
                .map_err(|reason| DriverSessionConfigError::invalid_node_addr(addr_str, reason)),
        }
    }
}

pub(crate) struct PyDuration(pub(crate) Duration);

impl<'py> FromPyObject<'_, 'py> for PyDuration {
    type Error = DriverSessionConfigError;
    fn extract(obj: Borrowed<'_, 'py, PyAny>) -> Result<Self, Self::Error> {
        if let Ok(duration) = obj.extract::<Duration>() {
            return Ok(PyDuration(duration));
        }

        if let Ok(secs) = obj.extract::<f64>() {
            let duration = Duration::try_from_secs_f64(secs)
                .map_err(|_| DriverSessionConfigError::invalid_duration(obj))?;
            return Ok(PyDuration(duration));
        }

        Err(DriverSessionConfigError::invalid_duration(obj))
    }
}

pub(crate) struct NodeAddrs {
    pub(crate) inner: Vec<NodeAddr>,
}

impl NodeAddrs {
    fn add_known_nodes(self, config: &mut SessionConfig) {
        for cp in self.inner.into_iter() {
            cp.add_known_node(config);
        }
    }
}

impl<'py> FromPyObject<'_, 'py> for NodeAddrs {
    type Error = DriverSessionConfigError;

    fn extract(obj: Borrowed<'_, 'py, PyAny>) -> Result<Self, Self::Error> {
        if let Ok(s) = obj.extract::<NodeAddr>() {
            return Ok(NodeAddrs { inner: vec![s] });
        }

        if let Ok(seq) = obj.cast::<PySequence>() {
            let iter = seq
                .try_iter()
                .map_err(DriverSessionConfigError::node_addr_iteration_failed)?;

            let list: Vec<NodeAddr> = iter
                .enumerate()
                .map(|(index, item_result)| {
                    let item = item_result
                        .map_err(|e| DriverSessionConfigError::invalid_node_addr_item(index, e))?;

                    item.extract::<NodeAddr>().map_err(|e| {
                        DriverSessionConfigError::invalid_node_addr_item(index, e.into())
                    })
                })
                .collect::<Result<Vec<_>, _>>()?;

            return Ok(NodeAddrs { inner: list });
        }

        Err(DriverSessionConfigError::address_type_error(obj))
    }
}

#[pymodule]
pub(crate) fn session_builder(_py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<SessionBuilder>()?;
    Ok(())
}
