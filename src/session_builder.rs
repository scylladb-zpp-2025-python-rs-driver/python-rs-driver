use crate::RUNTIME;
use crate::errors::{DriverSessionConfigError, DriverSessionConnectionError};
use crate::execution_profile::ExecutionProfile;
use crate::policies::{
    InternalAddressTranslator, InternalAuthenticatorProvider, InternalHostFilter,
    InternalTimestampGenerator, PyAddressTranslator, PyAuthenticatorProvider, PyHostFilter,
    PyTimestampGenerator,
};
use crate::session::Session;
use pyo3::prelude::*;
use pyo3::types::PySequence;
use scylla::authentication::PlainTextAuthenticator;
use scylla::client::session::SessionConfig;
use scylla::routing::ShardAwarePortRange;
use std::net::{IpAddr, SocketAddr};
use std::str::FromStr;
use std::ops::RangeInclusive;
use std::sync::Arc;

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
        contact_points: ContactPoints,
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
        authenticator: Py<PyAuthenticatorProvider>,
    ) -> PyRefMut<'py, Self> {
        slf.config.authenticator = Some(Arc::new(InternalAuthenticatorProvider {
            python_authenticator: authenticator,
        }));

        slf
    }

    fn address_translator<'py>(
        mut slf: PyRefMut<'py, Self>,
        translator: Py<PyAddressTranslator>,
    ) -> PyRefMut<'py, Self> {
        slf.config.address_translator = Some(Arc::new(InternalAddressTranslator {
            python_translator: translator,
        }));

        slf
    }

    fn timestamp_generator<'py>(
        mut slf: PyRefMut<'py, Self>,
        generator: Py<PyTimestampGenerator>,
    ) -> PyRefMut<'py, Self> {
        slf.config.timestamp_generator = Some(Arc::new(InternalTimestampGenerator {
            py_timestamp_generator: generator,
        }));

        slf
    }

    fn host_filter<'py>(
        mut slf: PyRefMut<'py, Self>,
        host_filter: Py<PyHostFilter>,
    ) -> PyRefMut<'py, Self> {
        slf.config.host_filter = Some(Arc::new(InternalHostFilter {
            py_host_filter: host_filter,
        }));

        slf
    }

    fn local_ip_address(mut slf: PyRefMut<'_, Self>, ip: Option<IpAddr>) -> PyRefMut<'_, Self> {
        slf.config.local_ip_address = ip;
        slf
    }

    fn shard_aware_local_port_range(
        mut slf: PyRefMut<'_, Self>,
        port_range: (u16, u16),
    ) -> Result<PyRefMut<'_, Self>, DriverSessionConfigError> {
        slf.config.shard_aware_local_port_range =
            ShardAwarePortRange::new(RangeInclusive::new(port_range.0, port_range.1))
                .map_err(|_| DriverSessionConfigError::InvalidPortRange)?;
        Ok(slf)
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

enum ContactPoint {
    Host(String),
    SocketAddr(SocketAddr),
}

impl ContactPoint {
    fn add_known_node(self, config: &mut SessionConfig) {
        match self {
            ContactPoint::Host(host) => config.add_known_node(host),
            ContactPoint::SocketAddr(addr) => config.add_known_node_addr(addr),
        }
    }
}

impl<'py> FromPyObject<'_, 'py> for ContactPoint {
    type Error = DriverSessionConfigError;

    fn extract(obj: Borrowed<'_, 'py, PyAny>) -> Result<Self, Self::Error> {
        if let Ok(s) = obj.extract::<String>() {
            return Ok(ContactPoint::Host(s));
        }

        if let Ok((host_str, port)) = obj.extract::<(&str, u16)>() {
            return if let Ok(ip) = IpAddr::from_str(host_str) {
                Ok(ContactPoint::SocketAddr(SocketAddr::new(ip, port)))
            } else {
                Ok(ContactPoint::Host(format!("{}:{}", host_str, port)))
            };
        }

        if let Ok((host, port)) = obj.extract::<(IpAddr, u16)>() {
            return Ok(ContactPoint::SocketAddr(SocketAddr::new(host, port)));
        }

        Err(DriverSessionConfigError::contact_point_type_error(obj))
    }
}

enum ContactPoints {
    Single(ContactPoint),
    Multiple(Vec<ContactPoint>),
}

impl ContactPoints {
    fn add_known_nodes(self, config: &mut SessionConfig) {
        match self {
            ContactPoints::Single(cp) => cp.add_known_node(config),
            ContactPoints::Multiple(seq) => {
                for cp in seq.into_iter() {
                    cp.add_known_node(config);
                }
            }
        }
    }
}

impl<'py> FromPyObject<'_, 'py> for ContactPoints {
    type Error = DriverSessionConfigError;

    fn extract(obj: Borrowed<'_, 'py, PyAny>) -> Result<Self, Self::Error> {
        if let Ok(s) = obj.extract::<ContactPoint>() {
            return Ok(ContactPoints::Single(s));
        }

        if let Ok(seq) = obj.cast::<PySequence>() {
            let iter = seq
                .try_iter()
                .map_err(DriverSessionConfigError::contact_points_iteration_failed)?;

            let list: Vec<ContactPoint> = iter
                .enumerate()
                .map(|(index, item_result)| {
                    let item = item_result.map_err(|e| {
                        DriverSessionConfigError::contact_points_invalid_item(index, e)
                    })?;

                    item.extract::<ContactPoint>().map_err(|e| {
                        DriverSessionConfigError::contact_points_invalid_item(index, e.into())
                    })
                })
                .collect::<Result<Vec<_>, _>>()?;

            return Ok(ContactPoints::Multiple(list));
        }

        Err(DriverSessionConfigError::contact_point_type_error(obj))
    }
}

#[pymodule]
pub(crate) fn session_builder(_py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<SessionBuilder>()?;
    Ok(())
}
